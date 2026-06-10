use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::logical_plan::{
    Aggregate, FetchType, Filter, Projection, SkipType, Sort, SubqueryAlias, Union,
};
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator};
use datafusion::optimizer::optimizer::{Optimizer, OptimizerContext};
use datafusion::optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, Result as DataFusionResult, plan_err};

use super::config::PlannerConfig;
use super::error::PlannerError;
use super::expr::combine_filters;

#[path = "logical_optimizer/helpers.rs"]
mod helpers;

use helpers::*;
const MAX_OPTIMIZER_PASSES: usize = 8;

pub(super) struct OptimizedLogicalPlan {
    pub(super) plan: LogicalPlan,
    pub(super) diagnostics: OptimizerDiagnostics,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OptimizerDiagnostics {
    stages: Vec<OptimizerStageDiagnostics>,
}

impl OptimizerDiagnostics {
    pub fn stages(&self) -> &[OptimizerStageDiagnostics] {
        &self.stages
    }

    pub fn total_applications(&self) -> usize {
        self.stages
            .iter()
            .map(OptimizerStageDiagnostics::total_applications)
            .sum()
    }

    pub fn rule_application_count(&self, rule_name: &str) -> usize {
        self.stages
            .iter()
            .flat_map(|stage| stage.rules.iter())
            .filter(|rule| rule.name == rule_name)
            .map(|rule| rule.count)
            .sum()
    }

    pub fn stage_application_count(&self, stage_name: &str) -> usize {
        self.stages
            .iter()
            .filter(|stage| stage.name == stage_name)
            .map(OptimizerStageDiagnostics::total_applications)
            .sum()
    }

    pub fn disabled_rules(&self) -> Vec<&'static str> {
        let mut disabled = BTreeSet::new();
        for stage in &self.stages {
            disabled.extend(stage.disabled_rules.iter().copied());
        }
        disabled.into_iter().collect()
    }

    pub fn max_passes_reached(&self) -> bool {
        self.stages.iter().any(|stage| stage.max_passes_reached)
    }

    fn stage_mut(&mut self, name: &'static str) -> &mut OptimizerStageDiagnostics {
        if let Some(index) = self.stages.iter().position(|stage| stage.name == name) {
            return &mut self.stages[index];
        }

        self.stages.push(OptimizerStageDiagnostics {
            name,
            passes: 0,
            max_passes_reached: false,
            rules: Vec::new(),
            disabled_rules: Vec::new(),
        });
        let index = self.stages.len() - 1;
        &mut self.stages[index]
    }
}

impl fmt::Display for OptimizerDiagnostics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for stage in &self.stages {
            if stage.total_applications() == 0
                && stage.disabled_rules.is_empty()
                && !stage.max_passes_reached
            {
                continue;
            }
            writeln!(f, "{}:", stage.name)?;
            if !stage.disabled_rules.is_empty() {
                writeln!(f, "  disabled {}", stage.disabled_rules.join(", "))?;
            }
            for rule in &stage.rules {
                writeln!(f, "  apply {} {} time(s)", rule.name, rule.count)?;
            }
            if stage.max_passes_reached {
                writeln!(
                    f,
                    "  reached max pass limit after {} pass(es)",
                    stage.passes
                )?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerStageDiagnostics {
    name: &'static str,
    passes: usize,
    max_passes_reached: bool,
    rules: Vec<OptimizerRuleDiagnostics>,
    disabled_rules: Vec<&'static str>,
}

impl OptimizerStageDiagnostics {
    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn passes(&self) -> usize {
        self.passes
    }

    pub fn max_passes_reached(&self) -> bool {
        self.max_passes_reached
    }

    pub fn rules(&self) -> &[OptimizerRuleDiagnostics] {
        &self.rules
    }

    pub fn disabled_rules(&self) -> &[&'static str] {
        &self.disabled_rules
    }

    pub fn total_applications(&self) -> usize {
        self.rules.iter().map(|rule| rule.count).sum()
    }

    fn set_disabled_rules(&mut self, disabled_rules: Vec<&'static str>) {
        self.disabled_rules = disabled_rules;
    }

    fn record_rule(&mut self, rule_name: &'static str) {
        if let Some(rule) = self.rules.iter_mut().find(|rule| rule.name == rule_name) {
            rule.count += 1;
            return;
        }

        self.rules.push(OptimizerRuleDiagnostics {
            name: rule_name,
            count: 1,
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerRuleDiagnostics {
    name: &'static str,
    count: usize,
}

impl OptimizerRuleDiagnostics {
    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn count(&self) -> usize {
        self.count
    }
}

pub(super) fn optimize_logical_plan(
    plan: &LogicalPlan,
    config: &PlannerConfig,
) -> Result<OptimizedLogicalPlan, PlannerError> {
    let mut current = optimize_scalar_subqueries(plan.clone())
        .map_err(|err| PlannerError::AnalysisError(err.into()))?;
    let mut diagnostics = OptimizerDiagnostics::default();

    for stage in optimizer_stages() {
        current = optimize_stage(current, &stage, config, &mut diagnostics)
            .map_err(|err| PlannerError::AnalysisError(err.into()))?;
    }

    if config.optimizer_diagnostics_enabled() && diagnostics.total_applications() > 0 {
        tracing::debug!(
            target: "dbsp_planner::logical_optimizer",
            diagnostics = %diagnostics,
            "optimized logical plan"
        );
    }

    Ok(OptimizedLogicalPlan {
        plan: current,
        diagnostics,
    })
}

fn optimize_scalar_subqueries(plan: LogicalPlan) -> DataFusionResult<LogicalPlan> {
    let optimizer = Optimizer::with_rules(vec![Arc::new(ScalarSubqueryToJoin::new())]);
    optimizer.optimize(plan, &OptimizerContext::new(), |_, _| {})
}

#[derive(Debug, Clone, Copy)]
enum ApplyOrder {
    TopDown,
    BottomUp,
}

struct OptimizerStage {
    name: &'static str,
    apply_order: ApplyOrder,
    max_passes: usize,
    rules: Vec<Box<dyn LogicalOptimizerRule>>,
}

impl OptimizerStage {
    fn fixed_point(
        name: &'static str,
        apply_order: ApplyOrder,
        rules: Vec<Box<dyn LogicalOptimizerRule>>,
    ) -> Self {
        Self {
            name,
            apply_order,
            max_passes: MAX_OPTIMIZER_PASSES,
            rules,
        }
    }
}

trait LogicalOptimizerRule: Send + Sync {
    fn name(&self) -> &'static str;

    fn apply(
        &self,
        plan: LogicalPlan,
        ctx: &RuleContext<'_>,
    ) -> DataFusionResult<Transformed<LogicalPlan>>;
}

struct RuleContext<'cfg> {
    config: &'cfg PlannerConfig,
}

fn optimizer_stages() -> Vec<OptimizerStage> {
    vec![
        OptimizerStage::fixed_point(
            "Normalize",
            ApplyOrder::BottomUp,
            vec![
                Box::new(MergeFiltersRule),
                Box::new(EliminateIdentityProjectionRule),
                Box::new(MergeProjectionsRule),
                Box::new(FlattenUnionsRule),
            ],
        ),
        OptimizerStage::fixed_point(
            "Pushdown",
            ApplyOrder::BottomUp,
            vec![
                Box::new(FilterProjectTransposeRule),
                Box::new(FilterSubqueryAliasTransposeRule),
                Box::new(FilterAggregateTransposeRule),
                Box::new(FilterUnionTransposeRule),
                Box::new(ProjectUnionTransposeRule),
            ],
        ),
        OptimizerStage::fixed_point(
            "Cleanup",
            ApplyOrder::BottomUp,
            vec![
                Box::new(MergeFiltersRule),
                Box::new(EliminateIdentityProjectionRule),
                Box::new(MergeProjectionsRule),
                Box::new(FlattenUnionsRule),
            ],
        ),
        OptimizerStage::fixed_point(
            "Prune",
            ApplyOrder::BottomUp,
            vec![
                Box::new(ProjectAggregatePruneRule),
                Box::new(ProjectFilterAggregatePruneRule),
            ],
        ),
        OptimizerStage::fixed_point(
            "FinalCleanup",
            ApplyOrder::BottomUp,
            vec![
                Box::new(MergeFiltersRule),
                Box::new(EliminateIdentityProjectionRule),
                Box::new(MergeProjectionsRule),
                Box::new(FlattenUnionsRule),
            ],
        ),
        OptimizerStage::fixed_point(
            "DbspPattern",
            ApplyOrder::TopDown,
            vec![Box::new(LimitSortToSortFetchRule)],
        ),
    ]
}

fn optimize_stage(
    mut current: LogicalPlan,
    stage: &OptimizerStage,
    config: &PlannerConfig,
    diagnostics: &mut OptimizerDiagnostics,
) -> DataFusionResult<LogicalPlan> {
    let disabled_rules = stage
        .rules
        .iter()
        .filter_map(|rule| (!config.optimizer_rule_enabled(rule.name())).then_some(rule.name()))
        .collect();
    diagnostics
        .stage_mut(stage.name)
        .set_disabled_rules(disabled_rules);

    if stage.rules.is_empty() {
        diagnostics.stage_mut(stage.name);
        return Ok(current);
    }

    for pass in 0..stage.max_passes {
        {
            let stage_diagnostics = diagnostics.stage_mut(stage.name);
            stage_diagnostics.passes = pass + 1;
        }

        let transformed = optimize_stage_pass(current, stage, config, diagnostics)?;
        current = transformed.data;
        if !transformed.transformed {
            return Ok(current);
        }
    }

    diagnostics.stage_mut(stage.name).max_passes_reached = true;
    Ok(current)
}

fn optimize_stage_pass(
    plan: LogicalPlan,
    stage: &OptimizerStage,
    config: &PlannerConfig,
    diagnostics: &mut OptimizerDiagnostics,
) -> DataFusionResult<Transformed<LogicalPlan>> {
    let rule_ctx = RuleContext { config };
    match stage.apply_order {
        ApplyOrder::TopDown => {
            let transformed = plan.transform_down(|node| {
                optimize_node_with_stage(node, stage, &rule_ctx, diagnostics)
            })?;
            Ok(transformed)
        }
        ApplyOrder::BottomUp => {
            let transformed = plan.transform_up(|node| {
                optimize_node_with_stage(node, stage, &rule_ctx, diagnostics)
            })?;
            Ok(transformed)
        }
    }
}

fn optimize_node_with_stage(
    plan: LogicalPlan,
    stage: &OptimizerStage,
    ctx: &RuleContext<'_>,
    diagnostics: &mut OptimizerDiagnostics,
) -> DataFusionResult<Transformed<LogicalPlan>> {
    let mut current = plan;
    for rule in &stage.rules {
        if !ctx.config.optimizer_rule_enabled(rule.name()) {
            continue;
        }

        let transformed = rule.apply(current, ctx)?;
        if transformed.transformed {
            diagnostics.stage_mut(stage.name).record_rule(rule.name());
            return Ok(transformed);
        }
        current = transformed.data;
    }

    Ok(Transformed::no(current))
}

struct MergeFiltersRule;

impl LogicalOptimizerRule for MergeFiltersRule {
    fn name(&self) -> &'static str {
        "MergeFilters"
    }

    fn apply(
        &self,
        plan: LogicalPlan,
        _ctx: &RuleContext<'_>,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Filter(filter) = plan else {
            return Ok(Transformed::no(plan));
        };

        if let LogicalPlan::Filter(inner) = filter.input.as_ref() {
            let predicate = and_expr(inner.predicate.clone(), filter.predicate);
            let merged = Filter::try_new(predicate, Arc::clone(&inner.input))?;
            return Ok(Transformed::yes(LogicalPlan::Filter(merged)));
        }

        Ok(Transformed::no(LogicalPlan::Filter(filter)))
    }
}

struct FilterProjectTransposeRule;

impl LogicalOptimizerRule for FilterProjectTransposeRule {
    fn name(&self) -> &'static str {
        "FilterProjectTranspose"
    }

    fn apply(
        &self,
        plan: LogicalPlan,
        _ctx: &RuleContext<'_>,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Filter(filter) = plan else {
            return Ok(Transformed::no(plan));
        };

        if let LogicalPlan::Projection(projection) = filter.input.as_ref()
            && !matches!(projection.input.as_ref(), LogicalPlan::Window(_))
            && !filter.predicate.is_volatile()
            && let Some(predicate) =
                rewrite_expr_through_projection(filter.predicate.clone(), projection, true)?
        {
            let pushed_filter =
                LogicalPlan::Filter(Filter::try_new(predicate, Arc::clone(&projection.input))?);
            let projection = projection_with_schema(
                projection.expr.clone(),
                Arc::new(pushed_filter),
                Arc::clone(&projection.schema),
            )?;
            return Ok(Transformed::yes(LogicalPlan::Projection(projection)));
        }

        Ok(Transformed::no(LogicalPlan::Filter(filter)))
    }
}

struct FilterSubqueryAliasTransposeRule;

impl LogicalOptimizerRule for FilterSubqueryAliasTransposeRule {
    fn name(&self) -> &'static str {
        "FilterSubqueryAliasTranspose"
    }

    fn apply(
        &self,
        plan: LogicalPlan,
        _ctx: &RuleContext<'_>,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Filter(filter) = plan else {
            return Ok(Transformed::no(plan));
        };

        if let LogicalPlan::SubqueryAlias(alias) = filter.input.as_ref()
            && !filter.predicate.is_volatile()
            && let Some(predicate) =
                rewrite_expr_through_subquery_alias(filter.predicate.clone(), alias)?
        {
            let pushed_filter =
                LogicalPlan::Filter(Filter::try_new(predicate, Arc::clone(&alias.input))?);
            let alias = SubqueryAlias::try_new(Arc::new(pushed_filter), alias.alias.clone())?;
            return Ok(Transformed::yes(LogicalPlan::SubqueryAlias(alias)));
        }

        Ok(Transformed::no(LogicalPlan::Filter(filter)))
    }
}

struct FilterUnionTransposeRule;

impl LogicalOptimizerRule for FilterUnionTransposeRule {
    fn name(&self) -> &'static str {
        "FilterUnionTranspose"
    }

    fn apply(
        &self,
        plan: LogicalPlan,
        ctx: &RuleContext<'_>,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Filter(filter) = plan else {
            return Ok(Transformed::no(plan));
        };

        if let LogicalPlan::Union(union) = filter.input.as_ref()
            && !filter.predicate.is_volatile()
            && can_duplicate_expressions(
                std::slice::from_ref(&filter.predicate),
                union.inputs.len(),
                ctx.config,
            )
            && let Some(inputs) = union
                .inputs
                .iter()
                .map(|input| {
                    Filter::try_new(filter.predicate.clone(), Arc::clone(input))
                        .map(|filter| Arc::new(LogicalPlan::Filter(filter)))
                        .ok()
                })
                .collect::<Option<Vec<_>>>()
        {
            let union = Union::try_new(inputs)?;
            return Ok(Transformed::yes(LogicalPlan::Union(union)));
        }

        Ok(Transformed::no(LogicalPlan::Filter(filter)))
    }
}

struct FilterAggregateTransposeRule;

impl LogicalOptimizerRule for FilterAggregateTransposeRule {
    fn name(&self) -> &'static str {
        "FilterAggregateTranspose"
    }

    fn apply(
        &self,
        plan: LogicalPlan,
        _ctx: &RuleContext<'_>,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Filter(filter) = plan else {
            return Ok(Transformed::no(plan));
        };

        let LogicalPlan::Aggregate(aggregate) = filter.input.as_ref() else {
            return Ok(Transformed::no(LogicalPlan::Filter(filter)));
        };

        if filter.predicate.is_volatile()
            || aggregate
                .group_expr
                .iter()
                .any(|expr| matches!(expr, Expr::GroupingSet(_)))
        {
            return Ok(Transformed::no(LogicalPlan::Filter(filter)));
        }

        let mut pushdown = Vec::new();
        let mut remaining = Vec::new();
        for conjunct in split_conjuncts(filter.predicate) {
            match rewrite_expr_through_aggregate_group_keys(conjunct.clone(), aggregate)? {
                Some(predicate) => pushdown.push(predicate),
                None => remaining.push(conjunct),
            }
        }

        let Some(pushed_predicate) = combine_filters(pushdown) else {
            let Some(predicate) = combine_filters(remaining) else {
                return plan_err!("aggregate filter pushdown found no predicate to keep or push");
            };
            return Ok(Transformed::no(LogicalPlan::Filter(Filter::try_new(
                predicate,
                Arc::clone(&filter.input),
            )?)));
        };

        let pushed_filter = LogicalPlan::Filter(Filter::try_new(
            pushed_predicate,
            Arc::clone(&aggregate.input),
        )?);
        let aggregate = aggregate_with_schema(
            aggregate.group_expr.clone(),
            aggregate.aggr_expr.clone(),
            Arc::new(pushed_filter),
            Arc::clone(&aggregate.schema),
        )?;
        let aggregate_plan = LogicalPlan::Aggregate(aggregate);

        if let Some(predicate) = combine_filters(remaining) {
            let filter = Filter::try_new(predicate, Arc::new(aggregate_plan))?;
            return Ok(Transformed::yes(LogicalPlan::Filter(filter)));
        }

        Ok(Transformed::yes(aggregate_plan))
    }
}

struct EliminateIdentityProjectionRule;

impl LogicalOptimizerRule for EliminateIdentityProjectionRule {
    fn name(&self) -> &'static str {
        "EliminateIdentityProjection"
    }

    fn apply(
        &self,
        plan: LogicalPlan,
        _ctx: &RuleContext<'_>,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Projection(projection) = plan else {
            return Ok(Transformed::no(plan));
        };

        if is_identity_projection(&projection) {
            return Ok(Transformed::yes(projection.input.as_ref().clone()));
        }

        Ok(Transformed::no(LogicalPlan::Projection(projection)))
    }
}

struct ProjectAggregatePruneRule;

impl LogicalOptimizerRule for ProjectAggregatePruneRule {
    fn name(&self) -> &'static str {
        "ProjectAggregatePrune"
    }

    fn apply(
        &self,
        plan: LogicalPlan,
        _ctx: &RuleContext<'_>,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Projection(projection) = plan else {
            return Ok(Transformed::no(plan));
        };

        let LogicalPlan::Aggregate(aggregate) = projection.input.as_ref() else {
            return Ok(Transformed::no(LogicalPlan::Projection(projection)));
        };

        let required_exprs = projection.expr.iter().collect::<Vec<_>>();
        let Some(pruned) = prune_aggregate_for_expressions(aggregate, &required_exprs)? else {
            return Ok(Transformed::no(LogicalPlan::Projection(projection)));
        };
        let projection_exprs = rewrite_exprs_for_pruned_aggregate(
            &projection.expr,
            &pruned.old_column_indices,
            &pruned.old_to_new_indices,
            &pruned.aggregate,
        )?;
        let projection = projection_with_schema(
            projection_exprs,
            Arc::new(LogicalPlan::Aggregate(pruned.aggregate)),
            Arc::clone(&projection.schema),
        )?;

        Ok(Transformed::yes(LogicalPlan::Projection(projection)))
    }
}

struct ProjectFilterAggregatePruneRule;

impl LogicalOptimizerRule for ProjectFilterAggregatePruneRule {
    fn name(&self) -> &'static str {
        "ProjectFilterAggregatePrune"
    }

    fn apply(
        &self,
        plan: LogicalPlan,
        _ctx: &RuleContext<'_>,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Projection(projection) = plan else {
            return Ok(Transformed::no(plan));
        };

        let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
            return Ok(Transformed::no(LogicalPlan::Projection(projection)));
        };
        let LogicalPlan::Aggregate(aggregate) = filter.input.as_ref() else {
            return Ok(Transformed::no(LogicalPlan::Projection(projection)));
        };

        let mut required_exprs = projection.expr.iter().collect::<Vec<_>>();
        required_exprs.push(&filter.predicate);
        let Some(pruned) = prune_aggregate_for_expressions(aggregate, &required_exprs)? else {
            return Ok(Transformed::no(LogicalPlan::Projection(projection)));
        };

        let predicate = rewrite_expr_for_pruned_aggregate(
            filter.predicate.clone(),
            &pruned.old_column_indices,
            &pruned.old_to_new_indices,
            &pruned.aggregate,
        )?;
        let projection_exprs = rewrite_exprs_for_pruned_aggregate(
            &projection.expr,
            &pruned.old_column_indices,
            &pruned.old_to_new_indices,
            &pruned.aggregate,
        )?;
        let aggregate_plan = LogicalPlan::Aggregate(pruned.aggregate);
        let filter = Filter::try_new(predicate, Arc::new(aggregate_plan))?;
        let projection = projection_with_schema(
            projection_exprs,
            Arc::new(LogicalPlan::Filter(filter)),
            Arc::clone(&projection.schema),
        )?;

        Ok(Transformed::yes(LogicalPlan::Projection(projection)))
    }
}

struct LimitSortToSortFetchRule;

impl LogicalOptimizerRule for LimitSortToSortFetchRule {
    fn name(&self) -> &'static str {
        "LimitSortToSortFetch"
    }

    fn apply(
        &self,
        plan: LogicalPlan,
        _ctx: &RuleContext<'_>,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Limit(limit) = plan else {
            return Ok(Transformed::no(plan));
        };

        let skip = match limit.get_skip_type()? {
            SkipType::Literal(skip) => skip,
            SkipType::UnsupportedExpr => return Ok(Transformed::no(LogicalPlan::Limit(limit))),
        };
        let fetch = match limit.get_fetch_type()? {
            FetchType::Literal(Some(fetch)) => fetch,
            FetchType::Literal(None) | FetchType::UnsupportedExpr => {
                return Ok(Transformed::no(LogicalPlan::Limit(limit)));
            }
        };
        if skip != 0 {
            return Ok(Transformed::no(LogicalPlan::Limit(limit)));
        }

        let LogicalPlan::Sort(sort) = limit.input.as_ref() else {
            return Ok(Transformed::no(LogicalPlan::Limit(limit)));
        };
        if sort.fetch.is_some() {
            return Ok(Transformed::no(LogicalPlan::Limit(limit)));
        }

        Ok(Transformed::yes(LogicalPlan::Sort(Sort {
            expr: sort.expr.clone(),
            input: Arc::clone(&sort.input),
            fetch: Some(fetch),
        })))
    }
}

struct MergeProjectionsRule;

impl LogicalOptimizerRule for MergeProjectionsRule {
    fn name(&self) -> &'static str {
        "MergeProjections"
    }

    fn apply(
        &self,
        plan: LogicalPlan,
        _ctx: &RuleContext<'_>,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Projection(projection) = plan else {
            return Ok(Transformed::no(plan));
        };

        if let LogicalPlan::Projection(inner) = projection.input.as_ref()
            && let Some(exprs) = projection
                .expr
                .iter()
                .map(|expr| rewrite_expr_through_projection(expr.clone(), inner, true))
                .collect::<DataFusionResult<Vec<_>>>()?
                .into_iter()
                .collect::<Option<Vec<_>>>()
        {
            let exprs = alias_exprs_to_schema(exprs, projection.schema.as_ref());
            let merged = projection_with_schema(
                exprs,
                Arc::clone(&inner.input),
                Arc::clone(&projection.schema),
            )?;
            return Ok(Transformed::yes(LogicalPlan::Projection(merged)));
        }

        Ok(Transformed::no(LogicalPlan::Projection(projection)))
    }
}

struct ProjectUnionTransposeRule;

impl LogicalOptimizerRule for ProjectUnionTransposeRule {
    fn name(&self) -> &'static str {
        "ProjectUnionTranspose"
    }

    fn apply(
        &self,
        plan: LogicalPlan,
        ctx: &RuleContext<'_>,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Projection(projection) = plan else {
            return Ok(Transformed::no(plan));
        };

        if let LogicalPlan::Union(union) = projection.input.as_ref()
            && projection.expr.iter().all(|expr| !expr.is_volatile())
            && can_duplicate_expressions(&projection.expr, union.inputs.len(), ctx.config)
            && let Some(inputs) = union
                .inputs
                .iter()
                .map(|input| {
                    let exprs =
                        alias_exprs_to_schema(projection.expr.clone(), projection.schema.as_ref());
                    projection_with_schema(exprs, Arc::clone(input), Arc::clone(&projection.schema))
                        .map(|projection| Arc::new(LogicalPlan::Projection(projection)))
                        .ok()
                })
                .collect::<Option<Vec<_>>>()
        {
            let union = Union::try_new(inputs)?;
            return Ok(Transformed::yes(LogicalPlan::Union(union)));
        }

        Ok(Transformed::no(LogicalPlan::Projection(projection)))
    }
}

struct FlattenUnionsRule;

impl LogicalOptimizerRule for FlattenUnionsRule {
    fn name(&self) -> &'static str {
        "FlattenUnions"
    }

    fn apply(
        &self,
        plan: LogicalPlan,
        _ctx: &RuleContext<'_>,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Union(union) = plan else {
            return Ok(Transformed::no(plan));
        };

        let mut changed = false;
        let mut inputs = Vec::with_capacity(union.inputs.len());
        for input in &union.inputs {
            if let LogicalPlan::Union(inner) = input.as_ref() {
                changed = true;
                inputs.extend(inner.inputs.iter().cloned());
            } else {
                inputs.push(Arc::clone(input));
            }
        }

        if changed {
            return Union::try_new(inputs)
                .map(LogicalPlan::Union)
                .map(Transformed::yes);
        }

        Ok(Transformed::no(LogicalPlan::Union(union)))
    }
}
