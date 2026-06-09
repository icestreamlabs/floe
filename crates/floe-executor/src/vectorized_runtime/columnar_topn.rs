use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::catalog::TableProvider;
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::logical_expr::expr::WindowFunction;
use datafusion::logical_expr::logical_plan::{Filter, Limit, Sort, TableScan, Window};
use datafusion::logical_expr::{Expr, LogicalPlan, Operator, ScalarUDF, WindowFunctionDefinition};
use datafusion::physical_plan::collect;
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarZSet};
use dbsp::storage::KeyValueTable;

use crate::delta_consolidation::diff_snapshot_batches;
use crate::mv::registry::MaterializedViewRegistry;
use crate::namespaces;
use crate::table_provider::DynamicStateTableProvider;
use crate::vectorized_runtime::source_state::{rename_batches, resolve_source_table};

use super::{
    VectorizedMaterializedViewState, VectorizedSourceState, apply_weighted_snapshot_delta,
    normalize_batches,
};

pub(super) struct ColumnarTopNPlan {
    logical_plan: LogicalPlan,
    source_name: String,
    partition_columns: Vec<String>,
}

pub(super) struct ColumnarTopNMaterializedViewState {
    source_name: String,
    source_schema: SchemaRef,
    input_zset: SlateBackedColumnarZSet,
    output_zset: SlateBackedColumnarZSet,
    evaluator: TopNEvaluator,
    partition_indices: Vec<usize>,
    partition_converter: RowConverter,
    source_snapshot: Vec<RecordBatch>,
    initial_snapshot: Vec<RecordBatch>,
}

impl ColumnarTopNMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        self.initial_snapshot.clone()
    }
}

struct TopNEvaluator {
    ctx: SessionContext,
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    provider: Arc<DynamicStateTableProvider>,
    alias_schema: Option<SchemaRef>,
    alias_provider: Option<Arc<DynamicStateTableProvider>>,
    output_schema: SchemaRef,
}

pub(super) fn columnar_topn_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<ColumnarTopNPlan>> {
    let partition_columns = if let Some((rank_column, filter)) = row_number_filter_for_plan(plan) {
        let Some((window, _projection_without_rank)) =
            extract_window_plan(filter.input.as_ref(), &rank_column)
        else {
            return Ok(None);
        };
        if window.window_expr.len() != 1 {
            return Ok(None);
        }
        let Some((_alias, window_function)) = row_number_window_function(&window.window_expr[0])
        else {
            return Ok(None);
        };
        if window_function.params.partition_by.is_empty() {
            return Ok(None);
        }
        let partition_columns = window_function
            .params
            .partition_by
            .iter()
            .map(partition_column_name)
            .collect::<Option<Vec<_>>>();
        let Some(partition_columns) = partition_columns else {
            return Ok(None);
        };
        partition_columns
    } else if global_sort_limit_for_plan(plan) {
        Vec::new()
    } else {
        return Ok(None);
    };
    let Some(source_name) = single_source_for_plan(plan, sources) else {
        return Ok(None);
    };
    if contains_unsupported_topn_wrapper(plan) {
        return Ok(None);
    }

    Ok(Some(ColumnarTopNPlan {
        logical_plan: plan.clone(),
        source_name,
        partition_columns,
    }))
}

pub(super) async fn build_columnar_topn_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarTopNPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarTopNMaterializedViewState> {
    let source = sources
        .get(&plan.source_name)
        .ok_or_else(|| anyhow::anyhow!("unknown topn source '{}'", plan.source_name))?;
    let partition_indices = plan
        .partition_columns
        .iter()
        .map(|column| partition_column_index(source, column))
        .collect::<Result<Vec<_>>>()?;
    let partition_converter = row_converter_for_indices(&source.schema, &partition_indices)?;
    let mv_namespace = namespaces::materialized_view(view_name)?;
    let input_namespace = format!("{mv_namespace}/columnar/topn/input");
    let output_namespace = format!("{mv_namespace}/columnar/topn/output");
    let input_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        input_namespace,
        Arc::clone(&source.schema),
    )
    .await
    .context("initialize SlateDB-backed topn input zset")?;
    let output_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        output_namespace,
        Arc::clone(output_schema),
    )
    .await
    .context("initialize SlateDB-backed topn output zset")?;
    let initial_snapshot = snapshot_batches_from_zset(
        &output_zset
            .materialize_columnar()
            .await
            .context("load topn output snapshot")?,
    )?;
    let source_snapshot = snapshot_batches_from_zset(
        &input_zset
            .materialize_columnar()
            .await
            .context("load topn input snapshot")?,
    )?;
    let evaluator = TopNEvaluator::build(
        plan.logical_plan,
        &plan.source_name,
        source,
        udfs,
        output_schema,
    )
    .await
    .context("build topn vectorized evaluator")?;

    Ok(ColumnarTopNMaterializedViewState {
        source_name: plan.source_name,
        source_schema: Arc::clone(&source.schema),
        input_zset,
        output_zset,
        evaluator,
        partition_indices,
        partition_converter,
        source_snapshot,
        initial_snapshot,
    })
}

pub(super) async fn run_columnar_topn_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    let Some(columnar) = mv.columnar_topn.as_mut() else {
        return Ok(false);
    };
    let plan_start = Instant::now();

    let input_delta = source_input_delta(columnar, insert_batches, weighted_delta_batches)?;
    let persisted_input_delta =
        persisted_source_delta(&mut columnar.input_zset, input_delta).await?;
    let touched_partitions = touched_partition_keys(
        &columnar.partition_converter,
        &columnar.partition_indices,
        persisted_input_delta.batches(),
    )?;
    let previous_source_for_keys = filter_batches_to_partition_keys(
        &columnar.source_schema,
        &columnar.partition_converter,
        &columnar.partition_indices,
        &columnar.source_snapshot,
        &touched_partitions,
    )?;
    let next_source_snapshot = apply_source_snapshot_delta(
        &columnar.source_schema,
        &columnar.source_snapshot,
        &persisted_input_delta,
    )
    .await?;
    let next_source_for_keys = filter_batches_to_partition_keys(
        &columnar.source_schema,
        &columnar.partition_converter,
        &columnar.partition_indices,
        &next_source_snapshot,
        &touched_partitions,
    )?;

    let previous_output = columnar
        .evaluator
        .evaluate(&previous_source_for_keys)
        .await
        .context("evaluate previous topn partition outputs")?;
    let next_output = columnar
        .evaluator
        .evaluate(&next_source_for_keys)
        .await
        .context("evaluate next topn partition outputs")?;
    let diff = diff_snapshot_batches(
        Arc::clone(&mv.output_schema),
        &previous_output,
        &next_output,
    )
    .await
    .context("diff topn partition outputs")?;

    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), diff.batches)
            .context("build topn output zset delta")?;
    let persisted_output_delta = if let Some(handle) = columnar
        .output_zset
        .create_version(
            &output_delta,
            columnar
                .output_zset
                .current_handle()
                .map(|handle| handle.version),
        )
        .await?
    {
        columnar.output_zset.read_delta(&handle).await?
    } else {
        output_delta
    };

    let delta_batches = persisted_output_delta.batches().to_vec();
    let next_snapshot = apply_weighted_snapshot_delta(
        &mv.output_schema,
        &mv.previous_snapshot,
        delta_batches.clone(),
    )
    .await
    .with_context(|| {
        format!(
            "apply Slate-backed topn columnar snapshot delta for '{}'",
            mv.view_name
        )
    })?;

    columnar.source_snapshot = next_source_snapshot;
    let handle = registry.register(mv.view_name.clone());
    handle.publish_arrow_version(version, next_snapshot.clone(), delta_batches);
    mv.previous_snapshot = next_snapshot;
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = "columnar_topn",
        "SlateDB-backed topn columnar DBSP materialized view tick completed"
    );
    Ok(true)
}

fn source_input_delta(
    columnar: &ColumnarTopNMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<ColumnarZSet> {
    if let Some(weighted_batches) = weighted_delta_batches.get(columnar.source_name.as_str()) {
        ColumnarZSet::try_new_weighted(
            Arc::clone(&columnar.source_schema),
            weighted_batches.clone(),
        )
        .with_context(|| {
            format!(
                "build weighted topn input delta for '{}'",
                columnar.source_name
            )
        })
    } else if let Some(source_batches) = insert_batches.get(columnar.source_name.as_str()) {
        ColumnarZSet::from_value_batches(
            Arc::clone(&columnar.source_schema),
            source_batches.clone(),
            1,
        )
        .with_context(|| {
            format!(
                "build insert topn input delta for '{}'",
                columnar.source_name
            )
        })
    } else {
        ColumnarZSet::empty(Arc::clone(&columnar.source_schema))
    }
}

async fn persisted_source_delta(
    zset: &mut SlateBackedColumnarZSet,
    input_delta: ColumnarZSet,
) -> Result<ColumnarZSet> {
    let base = zset.current_handle().map(|handle| handle.version);
    if let Some(handle) = zset.create_version(&input_delta, base).await? {
        zset.read_delta(&handle).await
    } else {
        Ok(input_delta)
    }
}

async fn apply_source_snapshot_delta(
    schema: &SchemaRef,
    previous: &[RecordBatch],
    delta: &ColumnarZSet,
) -> Result<Vec<RecordBatch>> {
    if delta.batches().is_empty() {
        return Ok(previous.to_vec());
    }
    apply_weighted_snapshot_delta(schema, previous, delta.batches().to_vec()).await
}

impl TopNEvaluator {
    async fn build(
        logical_plan: LogicalPlan,
        source_name: &str,
        source: &VectorizedSourceState,
        udfs: &[ScalarUDF],
        output_schema: &SchemaRef,
    ) -> Result<Self> {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        for udf in udfs.iter().cloned() {
            ctx.register_udf(udf);
        }
        let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(&source.schema)));
        let mut provider_by_table = HashMap::new();
        provider_by_table.insert(
            source_name.to_string(),
            Arc::clone(&provider) as Arc<dyn TableProvider>,
        );
        let (alias_schema, alias_provider) = if let (Some(alias), Some(alias_schema)) = (
            source_name.strip_prefix("nexmark_"),
            source.alias_schema.as_ref(),
        ) {
            let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(alias_schema)));
            provider_by_table.insert(
                alias.to_string(),
                Arc::clone(&provider) as Arc<dyn TableProvider>,
            );
            (Some(Arc::clone(alias_schema)), Some(provider))
        } else {
            (None, None)
        };
        let logical_plan = rebind_topn_logical_plan(logical_plan, &provider_by_table)?;
        let plan = ctx.state().create_physical_plan(&logical_plan).await?;
        Ok(Self {
            ctx,
            plan,
            provider,
            alias_schema,
            alias_provider,
            output_schema: Arc::clone(output_schema),
        })
    }

    async fn evaluate(&self, batches: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
        self.provider.set_batches(batches.to_vec())?;
        if let (Some(alias_schema), Some(alias_provider)) =
            (self.alias_schema.as_ref(), self.alias_provider.as_ref())
        {
            alias_provider.set_batches(rename_batches(batches, alias_schema)?)?;
        }
        let collected = collect(Arc::clone(&self.plan), self.ctx.task_ctx()).await;
        self.provider.set_batches(Vec::new())?;
        if let Some(alias_provider) = self.alias_provider.as_ref() {
            alias_provider.set_batches(Vec::new())?;
        }
        normalize_batches(
            collected.context("execute vectorized topn evaluator")?,
            &self.output_schema,
        )
    }
}

fn touched_partition_keys(
    converter: &RowConverter,
    partition_indices: &[usize],
    batches: &[RecordBatch],
) -> Result<HashSet<Vec<u8>>> {
    let mut keys = HashSet::new();
    if partition_indices.is_empty() {
        if batches.iter().any(|batch| batch.num_rows() > 0) {
            keys.insert(Vec::new());
        }
        return Ok(keys);
    }
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let rows = converter
            .convert_columns(&project_columns(batch, partition_indices))
            .context("encode touched topn partition keys")?;
        for row_idx in 0..batch.num_rows() {
            keys.insert(rows.row(row_idx).data().to_vec());
        }
    }
    Ok(keys)
}

fn filter_batches_to_partition_keys(
    schema: &SchemaRef,
    converter: &RowConverter,
    partition_indices: &[usize],
    batches: &[RecordBatch],
    keys: &HashSet<Vec<u8>>,
) -> Result<Vec<RecordBatch>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    if partition_indices.is_empty() {
        return Ok(batches.to_vec());
    }
    let mut output = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let rows = converter
            .convert_columns(&project_columns(batch, partition_indices))
            .context("encode topn snapshot partition keys")?;
        let mut indices = Vec::new();
        for row_idx in 0..batch.num_rows() {
            if keys.contains(rows.row(row_idx).data()) {
                indices.push(u32::try_from(row_idx).context("topn batch exceeds u32 rows")?);
            }
        }
        if indices.is_empty() {
            continue;
        }
        let indices = UInt32Array::from(indices);
        let columns = batch
            .columns()
            .iter()
            .map(|column| take(column.as_ref(), &indices, None))
            .collect::<std::result::Result<Vec<ArrayRef>, _>>()?;
        output.push(RecordBatch::try_new(Arc::clone(schema), columns)?);
    }
    Ok(output)
}

fn project_columns(batch: &RecordBatch, indices: &[usize]) -> Vec<ArrayRef> {
    indices
        .iter()
        .map(|idx| Arc::clone(batch.column(*idx)))
        .collect()
}

fn row_converter_for_indices(schema: &SchemaRef, indices: &[usize]) -> Result<RowConverter> {
    let fields = indices
        .iter()
        .map(|idx| SortField::new(schema.field(*idx).data_type().clone()))
        .collect::<Vec<_>>();
    RowConverter::new(fields).context("build topn partition Arrow row converter")
}

fn partition_column_index(source: &VectorizedSourceState, column: &str) -> Result<usize> {
    if let Ok(idx) = source.schema.index_of(column) {
        return Ok(idx);
    }
    if let Some(alias_schema) = source.alias_schema.as_ref()
        && let Ok(idx) = alias_schema.index_of(column)
    {
        return Ok(idx);
    }
    bail!("topn partition column '{column}' missing from source schema")
}

fn rebind_topn_logical_plan(
    logical_plan: LogicalPlan,
    provider_by_table: &HashMap<String, Arc<dyn TableProvider>>,
) -> Result<LogicalPlan> {
    let transformed = logical_plan.transform_up(|plan| match plan {
        LogicalPlan::TableScan(mut scan) => {
            let table_name = scan.table_name.table();
            let Some(provider) = provider_by_table.get(table_name) else {
                return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
            };
            scan.source = provider_as_source(Arc::clone(provider));
            Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
        }
        other => Ok(Transformed::no(other)),
    })?;
    Ok(transformed.data)
}

fn row_number_filter_for_plan(plan: &LogicalPlan) -> Option<(String, &Filter)> {
    match plan {
        LogicalPlan::Projection(projection) => {
            row_number_filter_for_plan(projection.input.as_ref())
        }
        LogicalPlan::Filter(filter) => {
            let (rank_column, _limit) = extract_row_number_limit(&filter.predicate)?;
            Some((rank_column, filter))
        }
        LogicalPlan::SubqueryAlias(alias) => row_number_filter_for_plan(alias.input.as_ref()),
        _ => None,
    }
}

fn global_sort_limit_for_plan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => {
            global_sort_limit_for_plan(projection.input.as_ref())
        }
        LogicalPlan::SubqueryAlias(alias) => global_sort_limit_for_plan(alias.input.as_ref()),
        LogicalPlan::Limit(limit) => {
            limit_has_nonnegative_skip_and_positive_fetch(limit)
                && sort_input_for_limit(limit.input.as_ref())
        }
        LogicalPlan::Sort(sort) => sort_has_positive_fetch(sort),
        _ => false,
    }
}

fn limit_has_nonnegative_skip_and_positive_fetch(limit: &Limit) -> bool {
    let skip = limit
        .skip
        .as_deref()
        .map(literal_to_nonnegative_usize)
        .unwrap_or(Some(0));
    let fetch = limit.fetch.as_deref().and_then(literal_to_positive_usize);
    skip.is_some() && fetch.is_some()
}

fn sort_input_for_limit(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::SubqueryAlias(alias) => sort_input_for_limit(alias.input.as_ref()),
        LogicalPlan::Sort(sort) => !sort.expr.is_empty(),
        _ => false,
    }
}

fn sort_has_positive_fetch(sort: &Sort) -> bool {
    !sort.expr.is_empty() && sort.fetch.is_some_and(|fetch| fetch > 0)
}

fn extract_row_number_limit(predicate: &Expr) -> Option<(String, usize)> {
    let Expr::BinaryExpr(binary) = predicate else {
        return None;
    };
    let (column, literal, exclusive) = match (&*binary.left, binary.op, &*binary.right) {
        (Expr::Column(column), Operator::LtEq, literal @ Expr::Literal(_, _)) => {
            (column.name.clone(), literal, false)
        }
        (Expr::Column(column), Operator::Lt, literal @ Expr::Literal(_, _)) => {
            (column.name.clone(), literal, true)
        }
        _ => return None,
    };
    let mut limit = literal_to_positive_usize(literal)?;
    if exclusive {
        if limit == 0 {
            return None;
        }
        limit -= 1;
    }
    (limit > 0).then_some((column, limit))
}

fn literal_to_nonnegative_usize(expr: &Expr) -> Option<usize> {
    literal_to_i128(expr)
        .filter(|value| *value >= 0)
        .and_then(|value| usize::try_from(value).ok())
}

fn literal_to_positive_usize(expr: &Expr) -> Option<usize> {
    literal_to_i128(expr)
        .filter(|value| *value > 0)
        .and_then(|value| usize::try_from(value).ok())
}

fn literal_to_i128(expr: &Expr) -> Option<i128> {
    let Expr::Literal(value, _) = expr else {
        return None;
    };
    match value {
        ScalarValue::Int8(Some(value)) => Some(i128::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(i128::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(i128::from(*value)),
        ScalarValue::Int64(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt8(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt64(Some(value)) => Some(i128::from(*value)),
        _ => None,
    }
}

fn extract_window_plan<'a>(
    input: &'a LogicalPlan,
    rank_column: &str,
) -> Option<(&'a Window, Option<Vec<Expr>>)> {
    let direct = strip_passthrough_wrappers(input);
    if let LogicalPlan::Window(window) = direct {
        return Some((window, None));
    }

    let projection = match direct {
        LogicalPlan::Projection(projection) => projection,
        _ => return None,
    };
    let window = match strip_passthrough_wrappers(projection.input.as_ref()) {
        LogicalPlan::Window(window) => window,
        _ => return None,
    };

    let mut saw_rank = false;
    let mut remaining = Vec::with_capacity(projection.expr.len());
    for expr in &projection.expr {
        if projection_expr_matches_rank(expr, rank_column) {
            saw_rank = true;
            continue;
        }
        remaining.push(expr.clone());
    }
    saw_rank.then_some((window, Some(remaining)))
}

fn strip_passthrough_wrappers(mut plan: &LogicalPlan) -> &LogicalPlan {
    loop {
        match plan {
            LogicalPlan::SubqueryAlias(alias) => {
                plan = alias.input.as_ref();
            }
            LogicalPlan::Repartition(repartition) => {
                plan = repartition.input.as_ref();
            }
            _ => return plan,
        }
    }
}

fn row_number_window_function(expr: &Expr) -> Option<(String, &WindowFunction)> {
    let (alias, window) = match expr {
        Expr::Alias(alias) => {
            let Expr::WindowFunction(window) = alias.expr.as_ref() else {
                return None;
            };
            (alias.name.clone(), window.as_ref())
        }
        Expr::WindowFunction(window) => (expr.schema_name().to_string(), window.as_ref()),
        _ => return None,
    };
    let is_row_number = matches!(
        &window.fun,
        WindowFunctionDefinition::WindowUDF(udf)
            if udf.name().eq_ignore_ascii_case("row_number")
    );
    if !is_row_number
        || window.params.filter.is_some()
        || window.params.null_treatment.is_some()
        || window.params.distinct
    {
        return None;
    }
    Some((alias, window))
}

fn partition_column_name(expr: &Expr) -> Option<String> {
    match strip_alias(expr) {
        Expr::Column(column) => Some(column.name.clone()),
        _ => None,
    }
}

fn projection_expr_matches_rank(expr: &Expr, rank_column: &str) -> bool {
    match expr {
        Expr::Column(column) => column.name == rank_column,
        Expr::Alias(alias) => alias.name == rank_column,
        _ => false,
    }
}

fn strip_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => strip_alias(alias.expr.as_ref()),
        _ => expr,
    }
}

fn single_source_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Option<String> {
    let sources = source_set_for_plan(plan, sources);
    if sources.len() == 1 {
        sources.into_iter().next()
    } else {
        None
    }
}

fn source_set_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    collect_sources(plan, sources, &mut out);
    out
}

fn collect_sources(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    out: &mut BTreeSet<String>,
) {
    match plan {
        LogicalPlan::TableScan(scan) => {
            if let Some(source_name) = table_scan_source(scan, sources) {
                out.insert(source_name);
            }
        }
        LogicalPlan::Projection(projection) => {
            collect_sources(projection.input.as_ref(), sources, out)
        }
        LogicalPlan::Filter(filter) => collect_sources(filter.input.as_ref(), sources, out),
        LogicalPlan::SubqueryAlias(alias) => collect_sources(alias.input.as_ref(), sources, out),
        LogicalPlan::Window(window) => collect_sources(window.input.as_ref(), sources, out),
        LogicalPlan::Limit(limit) => collect_sources(limit.input.as_ref(), sources, out),
        LogicalPlan::Sort(sort) => collect_sources(sort.input.as_ref(), sources, out),
        _ => {}
    }
}

fn table_scan_source(
    scan: &TableScan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Option<String> {
    resolve_source_table(scan.table_name.table().to_string(), sources)
}

fn contains_unsupported_topn_wrapper(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => {
            contains_unsupported_topn_wrapper(projection.input.as_ref())
        }
        LogicalPlan::Filter(filter) => contains_unsupported_topn_wrapper(filter.input.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => {
            contains_unsupported_topn_wrapper(alias.input.as_ref())
        }
        LogicalPlan::Window(window) => contains_unsupported_topn_wrapper(window.input.as_ref()),
        LogicalPlan::Limit(limit) => contains_unsupported_topn_wrapper(limit.input.as_ref()),
        LogicalPlan::Sort(sort) => contains_unsupported_topn_wrapper(sort.input.as_ref()),
        LogicalPlan::TableScan(_) => false,
        _ => true,
    }
}

fn snapshot_batches_from_zset(zset: &ColumnarZSet) -> Result<Vec<RecordBatch>> {
    let weight_idx = zset.weighted_schema().index_of(WEIGHT_COLUMN_NAME)?;
    let mut batches = zset
        .batches()
        .iter()
        .filter(|batch| batch.num_rows() > 0)
        .map(|batch| -> Result<RecordBatch> {
            let weights = batch
                .column(weight_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("columnar zset weight column must be Int64"))?;
            let mut indices = Vec::new();
            for row_idx in 0..weights.len() {
                if weights.is_null(row_idx) {
                    bail!("materialized columnar zset weight cannot be NULL");
                }
                let weight = weights.value(row_idx);
                if weight < 0 {
                    bail!("materialized columnar zset contains negative weight");
                }
                let row_idx =
                    u32::try_from(row_idx).context("columnar zset batch exceeds u32 rows")?;
                for _ in 0..weight {
                    indices.push(row_idx);
                }
            }
            let indices = UInt32Array::from(indices);
            let columns = batch
                .columns()
                .iter()
                .take(zset.value_column_count())
                .map(|column| take(column.as_ref(), &indices, None))
                .collect::<std::result::Result<Vec<ArrayRef>, _>>()?;
            Ok(RecordBatch::try_new(zset.value_schema(), columns)?)
        })
        .collect::<Result<Vec<_>>>()?;
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(zset.value_schema()));
    }
    Ok(batches)
}
