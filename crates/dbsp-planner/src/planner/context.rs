use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use datafusion::logical_expr::expr::Sort as ExprSort;
use datafusion::logical_expr::logical_plan::{FetchType, SkipType};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, Operator, WindowFunctionDefinition};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DataFusionError};

use dbsp_circuit::circuit::plan::{
    DbspAggregateNode, DbspDistinctNode, DbspJoinNode, DbspJoinType, DbspNodeKind, DbspProjectNode,
    DbspSelectNode, DbspSourceNode, DbspTopNNode, DbspUnionNode, DbspWindowAggregateNode,
    DbspWindowPolicy, DbspWindowSpec, OrderExpr, ProjectItem,
};
use dbsp_circuit::circuit::schema::{Field, RowSchema};
use dbsp_circuit::circuit::types::DbspScalarType;

use super::circuit::{CircuitNode, CircuitPlan};
use super::config::PlannerConfig;
use super::error::PlannerError;
use super::expr::{
    combine_filters, extract_alias, extract_join_keys_and_residual, map_aggregate_expr,
    normalize_expr,
};
use super::logical_optimizer::{OptimizerDiagnostics, optimize_logical_plan};

pub struct CircuitPlanner {
    config: PlannerConfig,
}

impl CircuitPlanner {
    pub fn new(config: PlannerConfig) -> Self {
        Self { config }
    }

    pub fn plan(&self, plan: &LogicalPlan) -> Result<CircuitPlan, PlannerError> {
        let plan = optimize_logical_plan(plan, &self.config)?.plan;
        let mut ctx = PlannerContext::new(&self.config);
        let planned = ctx.plan_node(&plan)?;
        Ok(CircuitPlan {
            root: planned.id,
            nodes: ctx.into_reachable_nodes(planned.id)?,
        })
    }

    pub fn optimize_logical_plan_with_diagnostics(
        &self,
        plan: &LogicalPlan,
    ) -> Result<(LogicalPlan, OptimizerDiagnostics), PlannerError> {
        let optimized = optimize_logical_plan(plan, &self.config)?;
        Ok((optimized.plan, optimized.diagnostics))
    }
}

struct PlannerContext<'cfg> {
    config: &'cfg PlannerConfig,
    nodes: Vec<CircuitNode>,
}

#[derive(Clone)]
struct PlannedNode {
    id: usize,
    schema: Arc<RowSchema>,
}

type RowNumberSpec = (String, Vec<Expr>, Vec<ExprSort>);
const DEFAULT_WINDOW_ALLOWED_LATENESS_MS: i64 = i64::MAX;

impl<'cfg> PlannerContext<'cfg> {
    fn new(config: &'cfg PlannerConfig) -> Self {
        Self {
            config,
            nodes: Vec::new(),
        }
    }

    fn into_reachable_nodes(self, root: usize) -> Result<Vec<CircuitNode>, PlannerError> {
        let mut reachable = BTreeSet::new();
        let mut stack = vec![root];
        while let Some(node_id) = stack.pop() {
            if !reachable.insert(node_id) {
                continue;
            }
            let node = self.node_by_id(node_id).ok_or_else(|| {
                PlannerError::UnsupportedPlan(format!(
                    "planned node {node_id} was not found during reachability pruning",
                ))
            })?;
            stack.extend(node.inputs.iter().copied());
        }

        Ok(self
            .nodes
            .into_iter()
            .filter(|node| reachable.contains(&node.id))
            .collect())
    }

    fn node_by_id(&self, id: usize) -> Option<&CircuitNode> {
        self.nodes.iter().find(|node| node.id == id)
    }

    fn plan_node(&mut self, plan: &LogicalPlan) -> Result<PlannedNode, PlannerError> {
        match plan {
            LogicalPlan::TableScan(scan) => {
                let table = self
                    .config
                    .table(&scan.table_name)
                    .ok_or_else(|| PlannerError::TableNotFound(scan.table_name.to_string()))?;
                let source_schema = table.schema().clone();
                let source_id = self.add_node(
                    vec![],
                    DbspNodeKind::Source(DbspSourceNode {
                        table: table.clone(),
                    }),
                    source_schema.clone(),
                );
                let mut current = PlannedNode {
                    id: source_id,
                    schema: source_schema.clone(),
                };

                if !scan.filters.is_empty() {
                    let predicates = scan
                        .filters
                        .iter()
                        .cloned()
                        .map(normalize_expr)
                        .collect::<Result<Vec<_>, _>>()?;
                    let predicate = combine_filters(predicates).ok_or_else(|| {
                        PlannerError::UnsupportedPlan(
                            "table scan filter list unexpectedly empty".to_string(),
                        )
                    })?;
                    let select = DbspSelectNode::try_new(source_schema.clone(), predicate)?;
                    let id = self.add_node(
                        vec![current.id],
                        DbspNodeKind::Select(select),
                        current.schema.clone(),
                    );
                    current.id = id;
                }

                if let Some(projection) = &scan.projection {
                    let mut items = Vec::with_capacity(projection.len());
                    for idx in projection {
                        let Some(field) = source_schema.field(*idx) else {
                            return Err(PlannerError::UnsupportedPlan(format!(
                                "table scan projection index {idx} out of bounds",
                            )));
                        };
                        items.push(ProjectItem {
                            expr: Expr::Column(Column::new_unqualified(field.name.clone())),
                            alias: Some(field.name.clone()),
                        });
                    }
                    let project = DbspProjectNode::try_new(current.schema.clone(), items)?;
                    let output_schema = project.output_schema().clone();
                    let id = self.add_node(
                        vec![current.id],
                        DbspNodeKind::Project(project),
                        output_schema.clone(),
                    );
                    current = PlannedNode {
                        id,
                        schema: output_schema,
                    };
                }

                Ok(current)
            }
            LogicalPlan::Projection(projection) => {
                let input = self.plan_node(&projection.input)?;
                self.build_projection_node(input, projection)
            }
            LogicalPlan::Filter(filter) => {
                if let Some(topn) = self.plan_row_number_filter(filter)? {
                    return Ok(topn);
                }
                let normalized_predicate = normalize_expr(filter.predicate.clone())?;
                if let LogicalPlan::Projection(projection) = filter.input.as_ref() {
                    let base = self.plan_node(&projection.input)?;
                    let predicate_references_only_base_columns = normalized_predicate
                        .column_refs()
                        .iter()
                        .all(|column| base.schema.field_index(column.name.as_str()).is_some());
                    if predicate_references_only_base_columns {
                        let select =
                            DbspSelectNode::try_new(base.schema.clone(), normalized_predicate)?;
                        let filter_id = self.add_node(
                            vec![base.id],
                            DbspNodeKind::Select(select),
                            base.schema.clone(),
                        );
                        let filtered = PlannedNode {
                            id: filter_id,
                            schema: base.schema.clone(),
                        };
                        return self.build_projection_node(filtered, projection);
                    }
                }

                let input = self.plan_node(&filter.input)?;
                if let Some(optimized) = self.optimize_join_subtree(
                    input.clone(),
                    Some(normalized_predicate.clone()),
                    None,
                )? {
                    return Ok(optimized);
                }
                let mut predicate_schema = input.schema.clone();
                if matches!(filter.input.as_ref(), LogicalPlan::Projection(_))
                    && let Some(node) = self.node_by_id(input.id)
                    && let DbspNodeKind::Project(project) = &node.kind
                    && normalized_predicate.column_refs().iter().all(|column| {
                        project
                            .input_schema()
                            .field_index(column.name.as_str())
                            .is_some()
                    })
                {
                    predicate_schema = Arc::clone(project.input_schema());
                }
                let select = DbspSelectNode::try_new(predicate_schema, normalized_predicate)?;
                let id = self.add_node(
                    vec![input.id],
                    DbspNodeKind::Select(select),
                    input.schema.clone(),
                );
                Ok(PlannedNode {
                    id,
                    schema: input.schema,
                })
            }
            LogicalPlan::Join(join) => self.plan_join(join),
            LogicalPlan::Aggregate(aggregate) => self.plan_aggregate(aggregate),
            LogicalPlan::Sort(sort) => {
                let input = self.plan_node(&sort.input)?;
                if let Some(fetch) = sort.fetch {
                    let order_by = self.map_sort_expressions(&sort.expr, input.schema.clone())?;
                    let topn = DbspTopNNode::try_new(
                        input.schema.clone(),
                        Vec::new(),
                        order_by,
                        fetch,
                        0,
                    )?;
                    let output_schema = topn.output_schema().clone();
                    let id = self.add_node(
                        vec![input.id],
                        DbspNodeKind::TopN(topn),
                        output_schema.clone(),
                    );
                    return Ok(PlannedNode {
                        id,
                        schema: output_schema,
                    });
                }
                let id = self.add_node(
                    vec![input.id],
                    DbspNodeKind::Passthrough,
                    input.schema.clone(),
                );
                Ok(PlannedNode {
                    id,
                    schema: input.schema,
                })
            }
            LogicalPlan::Limit(limit) => self.plan_limit(limit),
            LogicalPlan::Union(union) => self.plan_union(union),
            LogicalPlan::Window(_) => Err(PlannerError::UnsupportedPlan(
                "window logical plans must be translated via aggregate nodes".to_string(),
            )),
            LogicalPlan::SubqueryAlias(alias) => self.plan_node(&alias.input),
            LogicalPlan::Subquery(subquery) => self.plan_node(&subquery.subquery),
            LogicalPlan::Repartition(repartition) => self.plan_node(&repartition.input),
            LogicalPlan::Distinct(distinct) => self.plan_distinct(distinct),
            LogicalPlan::EmptyRelation(relation) => Err(PlannerError::UnsupportedPlan(format!(
                "empty relation nodes are not supported (produce_one_row = {})",
                relation.produce_one_row
            ))),
            LogicalPlan::Values(_) => Err(PlannerError::UnsupportedPlan(
                "VALUES lists are not supported".to_string(),
            )),
            LogicalPlan::Explain(_) => Err(PlannerError::UnsupportedPlan(
                "EXPLAIN plans are not supported".to_string(),
            )),
            LogicalPlan::Analyze(_) => Err(PlannerError::UnsupportedPlan(
                "ANALYZE plans are not supported".to_string(),
            )),
            LogicalPlan::Statement(_) => Err(PlannerError::UnsupportedPlan(
                "statement plans are not supported".to_string(),
            )),
            LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Extension(_) => Err(PlannerError::UnsupportedPlan(
                "plan type is not supported".to_string(),
            )),
            LogicalPlan::Copy(_) | LogicalPlan::RecursiveQuery(_) | LogicalPlan::Unnest(_) => Err(
                PlannerError::UnsupportedPlan("plan type is not supported".to_string()),
            ),
        }
    }

    fn build_projection_node(
        &mut self,
        input: PlannedNode,
        projection: &datafusion::logical_expr::logical_plan::Projection,
    ) -> Result<PlannedNode, PlannerError> {
        if let Some(optimized) =
            self.optimize_join_subtree(input.clone(), None, Some(&projection.expr))?
        {
            return Ok(optimized);
        }
        self.build_projection_items(input, &projection.expr)
    }

    fn build_projection_items(
        &mut self,
        input: PlannedNode,
        expressions: &[Expr],
    ) -> Result<PlannedNode, PlannerError> {
        let rewritten_expressions =
            self.rewrite_projection_expressions_for_input(&input, expressions)?;
        let items = rewritten_expressions
            .iter()
            .map(|expr| {
                let (expression, alias) = extract_alias(expr.clone())?;
                Ok(ProjectItem {
                    expr: expression,
                    alias,
                })
            })
            .collect::<Result<Vec<ProjectItem>, PlannerError>>()?;
        let project = DbspProjectNode::try_new(input.schema.clone(), items)?;
        let output_schema = project.output_schema().clone();
        let id = self.add_node(
            vec![input.id],
            DbspNodeKind::Project(project),
            output_schema.clone(),
        );
        Ok(PlannedNode {
            id,
            schema: output_schema,
        })
    }

    fn rewrite_projection_expressions_for_input(
        &self,
        input: &PlannedNode,
        expressions: &[Expr],
    ) -> Result<Vec<Expr>, PlannerError> {
        let join = self.join_node_for_projection_input(input.id);
        let Some(join) = join else {
            return Ok(expressions.to_vec());
        };
        let relation_sides = infer_join_relation_sides(Some(expressions), None, join);
        expressions
            .iter()
            .map(|expr| rewrite_join_output_projection_expr(expr.clone(), join, &relation_sides))
            .collect()
    }

    fn join_node_for_projection_input(&self, input_id: usize) -> Option<&DbspJoinNode> {
        let node = self.node_by_id(input_id)?;
        match &node.kind {
            DbspNodeKind::Join(join) => Some(join),
            DbspNodeKind::Select(_) => {
                let join_input_id = *node.inputs.first()?;
                let join_node = self.node_by_id(join_input_id)?;
                match &join_node.kind {
                    DbspNodeKind::Join(join) => Some(join),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn optimize_join_subtree(
        &mut self,
        input: PlannedNode,
        top_filter: Option<Expr>,
        projection_exprs: Option<&[Expr]>,
    ) -> Result<Option<PlannedNode>, PlannerError> {
        let Some(input_node) = self.node_by_id(input.id).cloned() else {
            return Ok(None);
        };

        let (join_input_id, current_top_filter) = match &input_node.kind {
            DbspNodeKind::Join(_) => (input.id, top_filter),
            DbspNodeKind::Select(select) => {
                let Some(join_input_id) = input_node.inputs.first().copied() else {
                    return Ok(None);
                };
                (
                    join_input_id,
                    combine_optional_filters(
                        top_filter,
                        Some(select.predicate().expression().expr().clone()),
                    ),
                )
            }
            _ => return Ok(None),
        };

        let Some(join_node) = self.node_by_id(join_input_id).cloned() else {
            return Ok(None);
        };
        let DbspNodeKind::Join(join) = &join_node.kind else {
            return Ok(None);
        };
        if !matches!(join.join_type, DbspJoinType::Inner) || join_node.inputs.len() != 2 {
            return Ok(None);
        }

        let join_relation_sides =
            infer_join_relation_sides(projection_exprs, current_top_filter.as_ref(), join);
        let rewritten_projection_exprs = projection_exprs
            .map(|expressions| {
                expressions
                    .iter()
                    .map(|expr| {
                        rewrite_join_output_projection_expr(
                            expr.clone(),
                            join,
                            &join_relation_sides,
                        )
                    })
                    .collect::<Result<Vec<_>, PlannerError>>()
            })
            .transpose()?;

        let required_output_columns =
            if let Some(expressions) = rewritten_projection_exprs.as_deref() {
                required_columns_for_expressions(expressions, input.schema.as_ref())?
            } else {
                (0..input.schema.len()).collect::<BTreeSet<_>>()
            };
        let SplitJoinFilter {
            left_pushdown,
            right_pushdown,
            remaining,
            required_columns,
        } = split_join_filter(
            current_top_filter.as_ref(),
            join.output_schema.as_ref(),
            join.left_schema.len(),
            join,
        )?;

        let mut left_required = BTreeSet::new();
        let mut right_required = BTreeSet::new();
        split_join_required_columns(
            &required_output_columns,
            join.left_schema.len(),
            &mut left_required,
            &mut right_required,
        )?;
        split_join_required_columns(
            &required_columns,
            join.left_schema.len(),
            &mut left_required,
            &mut right_required,
        )?;
        for key in &join.keys {
            add_required_expression_columns(
                key.left_expression().expr(),
                join.left_schema.as_ref(),
                &mut left_required,
            )?;
            add_required_expression_columns(
                key.right_expression().expr(),
                join.right_schema.as_ref(),
                &mut right_required,
            )?;
        }
        if let Some(residual) = &join.residual {
            let mut residual_columns = BTreeSet::new();
            add_required_expression_columns(
                residual.expr(),
                join.output_schema.as_ref(),
                &mut residual_columns,
            )?;
            split_join_required_columns(
                &residual_columns,
                join.left_schema.len(),
                &mut left_required,
                &mut right_required,
            )?;
        }

        let left_full = (0..join.left_schema.len()).collect::<BTreeSet<_>>();
        let right_full = (0..join.right_schema.len()).collect::<BTreeSet<_>>();
        let changed = left_pushdown.is_some()
            || right_pushdown.is_some()
            || remaining != current_top_filter
            || left_required != left_full
            || right_required != right_full;
        if !changed {
            return Ok(None);
        }

        let left_input = PlannedNode {
            id: join_node.inputs[0],
            schema: Arc::clone(&join.left_schema),
        };
        let right_input = PlannedNode {
            id: join_node.inputs[1],
            schema: Arc::clone(&join.right_schema),
        };

        let left = self.build_required_projection(
            left_input,
            Arc::clone(&join.left_schema),
            &left_required,
        )?;
        let right = self.build_required_projection(
            right_input,
            Arc::clone(&join.right_schema),
            &right_required,
        )?;
        let left = self.build_optional_select(left, left_pushdown)?;
        let right = self.build_optional_select(right, right_pushdown)?;

        let key_pairs = join
            .keys
            .iter()
            .map(|key| {
                (
                    key.left_expression().expr().clone(),
                    key.right_expression().expr().clone(),
                )
            })
            .collect::<Vec<_>>();
        let rebuilt_join = DbspJoinNode::try_new(
            DbspJoinType::Inner,
            left.schema.clone(),
            right.schema.clone(),
            key_pairs,
            join.residual
                .as_ref()
                .map(|residual| residual.expr().clone()),
        )?;
        let rebuilt_join_schema = rebuilt_join.output_schema.clone();
        let rebuilt_join_id = self.add_node(
            vec![left.id, right.id],
            DbspNodeKind::Join(rebuilt_join),
            rebuilt_join_schema.clone(),
        );
        let mut current = PlannedNode {
            id: rebuilt_join_id,
            schema: rebuilt_join_schema,
        };
        if let Some(remaining) = remaining {
            current = self.build_select(current, remaining)?;
        }
        if let Some(expressions) = rewritten_projection_exprs.as_deref() {
            return self.build_projection_items(current, expressions).map(Some);
        }
        Ok(Some(current))
    }

    fn build_required_projection(
        &mut self,
        input: PlannedNode,
        input_schema: Arc<RowSchema>,
        required_columns: &BTreeSet<usize>,
    ) -> Result<PlannedNode, PlannerError> {
        if required_columns.len() == input_schema.len() {
            return Ok(input);
        }
        let items = required_columns
            .iter()
            .map(|column_idx| {
                let field = input_schema.field(*column_idx).ok_or_else(|| {
                    PlannerError::UnsupportedPlan(format!(
                        "required projection column {column_idx} out of bounds",
                    ))
                })?;
                Ok(ProjectItem {
                    expr: Expr::Column(Column::new_unqualified(field.name.clone())),
                    alias: Some(field.name.clone()),
                })
            })
            .collect::<Result<Vec<_>, PlannerError>>()?;
        let project = DbspProjectNode::try_new(input_schema, items)?;
        let output_schema = project.output_schema().clone();
        let id = self.add_node(
            vec![input.id],
            DbspNodeKind::Project(project),
            output_schema.clone(),
        );
        Ok(PlannedNode {
            id,
            schema: output_schema,
        })
    }

    fn build_select(
        &mut self,
        input: PlannedNode,
        predicate: Expr,
    ) -> Result<PlannedNode, PlannerError> {
        let select = DbspSelectNode::try_new(input.schema.clone(), predicate)?;
        let id = self.add_node(
            vec![input.id],
            DbspNodeKind::Select(select),
            input.schema.clone(),
        );
        Ok(PlannedNode {
            id,
            schema: input.schema,
        })
    }

    fn build_optional_select(
        &mut self,
        input: PlannedNode,
        predicate: Option<Expr>,
    ) -> Result<PlannedNode, PlannerError> {
        match predicate {
            Some(predicate) => self.build_select(input, predicate),
            None => Ok(input),
        }
    }

    fn plan_row_number_filter(
        &mut self,
        filter: &datafusion::logical_expr::logical_plan::Filter,
    ) -> Result<Option<PlannedNode>, PlannerError> {
        let Some((rank_column, limit)) = extract_row_number_limit(&filter.predicate)? else {
            return Ok(None);
        };

        let (window_plan, post_projection) =
            match self.extract_window_plan(filter.input.as_ref(), &rank_column) {
                Some(found) => found,
                None => return Ok(None),
            };

        if window_plan.window_expr.len() != 1 {
            return Ok(None);
        }
        let Some((rank_alias, partition_by, order_by)) =
            self.parse_row_number_spec(&window_plan.window_expr[0])?
        else {
            return Ok(None);
        };

        if rank_column != rank_alias && post_projection.is_none() {
            return Ok(None);
        }

        let input = self.plan_node(&window_plan.input)?;
        let (partition_by, order_by, post_projection) = if let Some(join) =
            join_for_row_number_rewrite(self, input.id)
        {
            let mut join_relation_exprs = partition_by.clone();
            join_relation_exprs.extend(order_by.iter().map(|sort| sort.expr.clone()));
            if let Some(expressions) = post_projection.as_ref() {
                join_relation_exprs.extend(expressions.iter().cloned());
            }
            let join_relation_sides =
                infer_join_relation_sides(Some(join_relation_exprs.as_slice()), None, join);
            let rewritten_partition_by = partition_by
                .into_iter()
                .map(|expr| rewrite_join_output_projection_expr(expr, join, &join_relation_sides))
                .collect::<Result<Vec<_>, PlannerError>>()?;
            let rewritten_order_by = order_by
                .into_iter()
                .map(|sort| {
                    Ok(ExprSort {
                        expr: rewrite_join_output_projection_expr(
                            sort.expr,
                            join,
                            &join_relation_sides,
                        )?,
                        asc: sort.asc,
                        nulls_first: sort.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>, PlannerError>>()?;
            let rewritten_post_projection = post_projection
                .map(|expressions| {
                    expressions
                        .into_iter()
                        .map(|expr| {
                            rewrite_join_output_projection_expr(expr, join, &join_relation_sides)
                        })
                        .collect::<Result<Vec<_>, PlannerError>>()
                })
                .transpose()?;
            (
                rewritten_partition_by,
                rewritten_order_by,
                rewritten_post_projection,
            )
        } else {
            (partition_by, order_by, post_projection)
        };
        let order_by = self.map_sort_expressions(&order_by, input.schema.clone())?;
        let topn = DbspTopNNode::try_new(input.schema.clone(), partition_by, order_by, limit, 0)?;
        let output_schema = topn.output_schema().clone();
        let id = self.add_node(
            vec![input.id],
            DbspNodeKind::TopN(topn),
            output_schema.clone(),
        );
        let topn_node = PlannedNode {
            id,
            schema: output_schema,
        };

        if let Some(exprs) = post_projection {
            return self.build_projection_items(topn_node, &exprs).map(Some);
        }

        Ok(Some(topn_node))
    }

    fn plan_join(
        &mut self,
        join: &datafusion::logical_expr::logical_plan::Join,
    ) -> Result<PlannedNode, PlannerError> {
        let join_type = match join.join_type {
            JoinType::Inner => DbspJoinType::Inner,
            JoinType::Left => DbspJoinType::LeftOuter,
            JoinType::Right => DbspJoinType::RightOuter,
            JoinType::Full => DbspJoinType::FullOuter,
            other => {
                return Err(PlannerError::UnsupportedJoin(format!(
                    "unsupported join type {other:?}; only INNER/LEFT/RIGHT/FULL OUTER joins are supported"
                )));
            }
        };

        let left = self.plan_node(&join.left)?;
        let right = self.plan_node(&join.right)?;

        let mut key_pairs = join
            .on
            .iter()
            .map(|(left_expr, right_expr)| {
                let left = normalize_expr(left_expr.clone())?;
                let right = normalize_expr(right_expr.clone())?;
                Ok((left, right))
            })
            .collect::<Result<Vec<_>, PlannerError>>()?;

        let mut residuals: Vec<Expr> = Vec::new();
        if let Some(filter_expr) = &join.filter {
            let (filter_keys, filter_residual) = extract_join_keys_and_residual(filter_expr)?;
            key_pairs.extend(filter_keys);
            if let Some(expr) = filter_residual {
                residuals.push(expr);
            }
        }

        if key_pairs.is_empty() {
            return Err(PlannerError::UnsupportedJoin(
                "joins must have at least one equi-key".to_string(),
            ));
        }

        let residual = combine_filters(residuals);
        if !matches!(join_type, DbspJoinType::Inner) && residual.is_some() {
            return Err(PlannerError::UnsupportedJoin(
                "OUTER joins currently require pure equi-join predicates".to_string(),
            ));
        }

        let join_node = DbspJoinNode::try_new(
            join_type,
            left.schema.clone(),
            right.schema.clone(),
            key_pairs,
            residual,
        )?;
        let output_schema = join_node.output_schema.clone();
        let id = self.add_node(
            vec![left.id, right.id],
            DbspNodeKind::Join(join_node),
            output_schema.clone(),
        );
        Ok(PlannedNode {
            id,
            schema: output_schema,
        })
    }

    fn plan_aggregate(
        &mut self,
        aggregate: &datafusion::logical_expr::logical_plan::Aggregate,
    ) -> Result<PlannedNode, PlannerError> {
        let input = self.plan_node(&aggregate.input)?;

        if aggregate.group_expr.is_empty() && aggregate.aggr_expr.is_empty() {
            return Err(PlannerError::UnsupportedPlan(
                "aggregate nodes must have group or aggregate expressions".to_string(),
            ));
        }

        let mut group_keys = Vec::with_capacity(aggregate.group_expr.len());
        let mut window_spec: Option<DbspWindowSpec> = None;
        for expr in &aggregate.group_expr {
            if let Some(spec) = self.parse_window_spec(expr, input.schema.clone())? {
                if window_spec.is_some() {
                    return Err(PlannerError::UnsupportedPlan(
                        "only one window specification is supported in GROUP BY".to_string(),
                    ));
                }
                window_spec = Some(spec);
                continue;
            }
            let default_alias = match expr {
                Expr::Column(column) => column.name.clone(),
                Expr::Alias(alias) => match alias.expr.as_ref() {
                    Expr::Column(column) => column.name.clone(),
                    _ => expr.schema_name().to_string(),
                },
                _ => expr.schema_name().to_string(),
            };
            let (group_expr, alias) = extract_alias(expr.clone())?;
            group_keys.push((group_expr, alias.or(Some(default_alias))));
        }

        let aggregates = aggregate
            .aggr_expr
            .iter()
            .map(map_aggregate_expr)
            .collect::<Result<Vec<_>, _>>()?;

        let agg_node = DbspAggregateNode::try_new(input.schema.clone(), group_keys, aggregates)?;

        if let Some(window_spec) = window_spec {
            let output_schema = self.window_output_schema(&agg_node, &window_spec)?;
            let window_node = DbspWindowAggregateNode {
                aggregate: agg_node,
                window: window_spec,
            };
            let id = self.add_node(
                vec![input.id],
                DbspNodeKind::WindowAggregate(window_node),
                output_schema.clone(),
            );
            return Ok(PlannedNode {
                id,
                schema: output_schema,
            });
        }

        let output_schema = agg_node.output_schema().clone();
        let id = self.add_node(
            vec![input.id],
            DbspNodeKind::Aggregate(agg_node),
            output_schema.clone(),
        );
        Ok(PlannedNode {
            id,
            schema: output_schema,
        })
    }

    fn plan_limit(
        &mut self,
        limit: &datafusion::logical_expr::logical_plan::Limit,
    ) -> Result<PlannedNode, PlannerError> {
        let fetch = match limit
            .get_fetch_type()
            .map_err(|err| PlannerError::UnsupportedPlan(err.to_string()))?
        {
            FetchType::Literal(Some(value)) if value > 0 => value,
            FetchType::Literal(Some(_)) => {
                return Err(PlannerError::UnsupportedPlan(
                    "LIMIT must be a positive literal".to_string(),
                ));
            }
            FetchType::Literal(None) => {
                return Err(PlannerError::UnsupportedPlan(
                    "LIMIT without FETCH is not supported".to_string(),
                ));
            }
            FetchType::UnsupportedExpr => {
                return Err(PlannerError::UnsupportedPlan(
                    "LIMIT expressions must be literal integers".to_string(),
                ));
            }
        };
        let offset = match limit
            .get_skip_type()
            .map_err(|err| PlannerError::UnsupportedPlan(err.to_string()))?
        {
            SkipType::Literal(value) => value,
            SkipType::UnsupportedExpr => {
                return Err(PlannerError::UnsupportedPlan(
                    "OFFSET expressions must be literal integers".to_string(),
                ));
            }
        };

        if let LogicalPlan::Sort(sort) = limit.input.as_ref() {
            let input = self.plan_node(&sort.input)?;
            let order_by = self.map_sort_expressions(&sort.expr, input.schema.clone())?;
            let topn =
                DbspTopNNode::try_new(input.schema.clone(), Vec::new(), order_by, fetch, offset)?;
            let output_schema = topn.output_schema().clone();
            let id = self.add_node(
                vec![input.id],
                DbspNodeKind::TopN(topn),
                output_schema.clone(),
            );
            return Ok(PlannedNode {
                id,
                schema: output_schema,
            });
        }

        Err(PlannerError::UnsupportedPlan(
            "LIMIT requires an ORDER BY to form a TopN operator".to_string(),
        ))
    }

    fn parse_row_number_spec(&self, expr: &Expr) -> Result<Option<RowNumberSpec>, PlannerError> {
        let (alias, window) = match expr {
            Expr::Alias(alias) => {
                let Expr::WindowFunction(window) = alias.expr.as_ref() else {
                    return Ok(None);
                };
                (alias.name.clone(), window)
            }
            Expr::WindowFunction(window) => (expr.schema_name().to_string(), window),
            _ => return Ok(None),
        };
        let is_row_number = matches!(
            &window.fun,
            WindowFunctionDefinition::WindowUDF(udf)
                if udf.name().eq_ignore_ascii_case("row_number")
        );
        if !is_row_number {
            return Ok(None);
        }
        if window.params.filter.is_some()
            || window.params.null_treatment.is_some()
            || window.params.distinct
        {
            return Ok(None);
        }

        let partition_by = window
            .params
            .partition_by
            .iter()
            .cloned()
            .map(normalize_expr)
            .collect::<Result<Vec<_>, _>>()?;
        let order_by = window.params.order_by.clone();
        Ok(Some((alias, partition_by, order_by)))
    }

    fn strip_passthrough_wrappers<'a>(&self, mut plan: &'a LogicalPlan) -> &'a LogicalPlan {
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

    fn extract_window_plan<'a>(
        &self,
        input: &'a LogicalPlan,
        rank_column: &str,
    ) -> Option<(
        &'a datafusion::logical_expr::logical_plan::Window,
        Option<Vec<Expr>>,
    )> {
        let direct = self.strip_passthrough_wrappers(input);
        if let LogicalPlan::Window(window) = direct {
            return Some((window, None));
        }

        let projection = match direct {
            LogicalPlan::Projection(projection) => projection,
            _ => return None,
        };
        let window = match self.strip_passthrough_wrappers(projection.input.as_ref()) {
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
        if !saw_rank {
            return None;
        }
        Some((window, Some(remaining)))
    }

    fn plan_union(
        &mut self,
        union: &datafusion::logical_expr::logical_plan::Union,
    ) -> Result<PlannedNode, PlannerError> {
        let mut input_nodes = Vec::with_capacity(union.inputs.len());
        let mut schemas = Vec::with_capacity(union.inputs.len());
        for input in &union.inputs {
            let planned = self.plan_node(input)?;
            schemas.push(planned.schema.clone());
            input_nodes.push(planned);
        }

        if schemas.is_empty() {
            return Err(PlannerError::UnsupportedPlan(
                "union requires at least one input".to_string(),
            ));
        }

        let union_node = DbspUnionNode::try_new(schemas)?;
        let output_schema = union_node.output_schema().clone();
        let input_ids = input_nodes.iter().map(|node| node.id).collect::<Vec<_>>();
        let id = self.add_node(
            input_ids,
            DbspNodeKind::Union(union_node),
            output_schema.clone(),
        );
        Ok(PlannedNode {
            id,
            schema: output_schema,
        })
    }

    fn plan_distinct(
        &mut self,
        distinct: &datafusion::logical_expr::logical_plan::Distinct,
    ) -> Result<PlannedNode, PlannerError> {
        let input_plan = match distinct {
            datafusion::logical_expr::logical_plan::Distinct::All(input) => input,
            datafusion::logical_expr::logical_plan::Distinct::On(_) => {
                return Err(PlannerError::UnsupportedPlan(
                    "DISTINCT ON is not supported".to_string(),
                ));
            }
        };
        let input = self.plan_node(input_plan)?;
        let distinct_node = DbspDistinctNode::new(input.schema.clone());
        let output_schema = distinct_node.output_schema().clone();
        let id = self.add_node(
            vec![input.id],
            DbspNodeKind::Distinct(distinct_node),
            output_schema.clone(),
        );
        Ok(PlannedNode {
            id,
            schema: output_schema,
        })
    }

    fn parse_window_spec(
        &self,
        expr: &Expr,
        input_schema: Arc<RowSchema>,
    ) -> Result<Option<DbspWindowSpec>, PlannerError> {
        let expr = match expr {
            Expr::Alias(alias) => alias.expr.as_ref(),
            _ => expr,
        };
        let Expr::ScalarFunction(func) = expr else {
            return Ok(None);
        };

        let name = func.name().to_ascii_lowercase();
        match name.as_str() {
            "tumble" => {
                if !matches!(func.args.len(), 2 | 3) {
                    return Err(PlannerError::UnsupportedPlan(
                        "TUMBLE requires (time_expr, size_ms[, allowed_lateness_ms]) arguments"
                            .to_string(),
                    ));
                }
                let time_expr = normalize_expr(func.args[0].clone())?;
                let size_ms = self.parse_window_arg(&func.args[1])?;
                let allowed_lateness_ms = if func.args.len() == 3 {
                    self.parse_window_arg(&func.args[2])?
                } else {
                    DEFAULT_WINDOW_ALLOWED_LATENESS_MS
                };
                let spec = DbspWindowSpec::try_new(
                    DbspWindowPolicy::Tumbling { size_ms },
                    time_expr,
                    input_schema,
                    allowed_lateness_ms,
                )?;
                Ok(Some(spec))
            }
            "hop" => {
                if !matches!(func.args.len(), 3 | 4) {
                    return Err(PlannerError::UnsupportedPlan(
                        "HOP requires (time_expr, slide_ms, size_ms[, allowed_lateness_ms]) arguments"
                            .to_string(),
                    ));
                }
                let time_expr = normalize_expr(func.args[0].clone())?;
                let slide_ms = self.parse_window_arg(&func.args[1])?;
                let size_ms = self.parse_window_arg(&func.args[2])?;
                let allowed_lateness_ms = if func.args.len() == 4 {
                    self.parse_window_arg(&func.args[3])?
                } else {
                    DEFAULT_WINDOW_ALLOWED_LATENESS_MS
                };
                let spec = DbspWindowSpec::try_new(
                    DbspWindowPolicy::Hopping { size_ms, slide_ms },
                    time_expr,
                    input_schema,
                    allowed_lateness_ms,
                )?;
                Ok(Some(spec))
            }
            "session" => {
                if !matches!(func.args.len(), 2 | 3) {
                    return Err(PlannerError::UnsupportedPlan(
                        "SESSION requires (time_expr, gap_ms[, allowed_lateness_ms]) arguments"
                            .to_string(),
                    ));
                }
                let time_expr = normalize_expr(func.args[0].clone())?;
                let gap_ms = self.parse_window_arg(&func.args[1])?;
                let allowed_lateness_ms = if func.args.len() == 3 {
                    self.parse_window_arg(&func.args[2])?
                } else {
                    DEFAULT_WINDOW_ALLOWED_LATENESS_MS
                };
                let spec = DbspWindowSpec::try_new(
                    DbspWindowPolicy::Session { gap_ms },
                    time_expr,
                    input_schema,
                    allowed_lateness_ms,
                )?;
                Ok(Some(spec))
            }
            _ => Ok(None),
        }
    }

    fn parse_window_arg(&self, expr: &Expr) -> Result<i64, PlannerError> {
        let Expr::Literal(value, _) = expr else {
            return Err(PlannerError::UnsupportedPlan(
                "window sizes must be literal Int64 milliseconds".to_string(),
            ));
        };
        let array = value.to_array().map_err(|_| {
            PlannerError::UnsupportedPlan(
                "window sizes must be literal Int64 milliseconds".to_string(),
            )
        })?;
        let values = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
            PlannerError::UnsupportedPlan(
                "window sizes must be literal Int64 milliseconds".to_string(),
            )
        })?;
        if values.is_null(0) {
            return Err(PlannerError::UnsupportedPlan(
                "window sizes must be literal Int64 milliseconds".to_string(),
            ));
        }
        Ok(values.value(0))
    }

    fn window_output_schema(
        &self,
        aggregate: &DbspAggregateNode,
        window_spec: &DbspWindowSpec,
    ) -> Result<Arc<RowSchema>, PlannerError> {
        let nullable = window_spec.time_expression.nullable();
        let mut fields = Vec::with_capacity(2 + aggregate.output_schema().fields().len());
        fields.push(Field::new(
            "window_start",
            DbspScalarType::TimestampMillis,
            nullable,
        ));
        fields.push(Field::new(
            "window_end",
            DbspScalarType::TimestampMillis,
            nullable,
        ));
        fields.extend(aggregate.output_schema().fields().iter().cloned());
        RowSchema::try_new(fields).map_err(PlannerError::from)
    }

    fn add_node(
        &mut self,
        inputs: Vec<usize>,
        kind: DbspNodeKind,
        schema: Arc<RowSchema>,
    ) -> usize {
        let id = self.nodes.len();
        self.nodes.push(CircuitNode {
            id,
            kind,
            inputs,
            output_schema: schema.clone(),
        });
        id
    }

    fn map_sort_expressions(
        &self,
        expressions: &[ExprSort],
        input_schema: Arc<RowSchema>,
    ) -> Result<Vec<OrderExpr>, PlannerError> {
        expressions
            .iter()
            .map(|sort| {
                let expr = normalize_expr(sort.expr.clone())?;
                OrderExpr::try_new(expr, input_schema.clone(), sort.asc, sort.nulls_first)
                    .map_err(PlannerError::from)
            })
            .collect()
    }
}

fn join_for_row_number_rewrite<'a>(
    ctx: &'a PlannerContext<'a>,
    input_id: usize,
) -> Option<&'a DbspJoinNode> {
    let input_node = ctx.node_by_id(input_id)?;
    match &input_node.kind {
        DbspNodeKind::Join(join) => Some(join),
        DbspNodeKind::Select(_) => {
            let join_input_id = input_node.inputs.first().copied()?;
            let join_node = ctx.node_by_id(join_input_id)?;
            match &join_node.kind {
                DbspNodeKind::Join(join) => Some(join),
                _ => None,
            }
        }
        _ => None,
    }
}

#[derive(Debug, Default)]
struct SplitJoinFilter {
    left_pushdown: Option<Expr>,
    right_pushdown: Option<Expr>,
    remaining: Option<Expr>,
    required_columns: BTreeSet<usize>,
}

#[derive(Clone, Copy)]
enum JoinInputSide {
    Left,
    Right,
}

fn combine_optional_filters(left: Option<Expr>, right: Option<Expr>) -> Option<Expr> {
    let mut filters = Vec::new();
    if let Some(left) = left {
        filters.push(left);
    }
    if let Some(right) = right {
        filters.push(right);
    }
    combine_filters(filters)
}

fn required_columns_for_expressions(
    expressions: &[Expr],
    input_schema: &RowSchema,
) -> Result<BTreeSet<usize>, PlannerError> {
    let mut columns = BTreeSet::new();
    for expr in expressions {
        let (expression, _) = extract_alias(expr.clone())?;
        add_required_expression_columns(&expression, input_schema, &mut columns)?;
    }
    Ok(columns)
}

fn infer_join_relation_sides(
    projection_exprs: Option<&[Expr]>,
    top_filter: Option<&Expr>,
    join: &DbspJoinNode,
) -> HashMap<String, JoinInputSide> {
    let mut inferred = HashMap::new();
    if let Some(expressions) = projection_exprs {
        for expr in expressions {
            accumulate_join_relation_sides(expr, join, &mut inferred);
        }
    }
    if let Some(filter) = top_filter {
        accumulate_join_relation_sides(filter, join, &mut inferred);
    }
    inferred
}

fn accumulate_join_relation_sides(
    expression: &Expr,
    join: &DbspJoinNode,
    inferred: &mut HashMap<String, JoinInputSide>,
) {
    for column in expression.column_refs() {
        let Some(relation) = column.relation.as_ref().map(ToString::to_string) else {
            continue;
        };
        let left_has = join.left_schema.field_index(column.name.as_str()).is_some();
        let right_has = join
            .right_schema
            .field_index(column.name.as_str())
            .is_some();
        let Some(side) = (match (left_has, right_has) {
            (true, false) => Some(JoinInputSide::Left),
            (false, true) => Some(JoinInputSide::Right),
            _ => None,
        }) else {
            continue;
        };
        inferred.entry(relation).or_insert(side);
    }
}

fn rewrite_join_output_projection_expr(
    expression: Expr,
    join: &DbspJoinNode,
    relation_sides: &HashMap<String, JoinInputSide>,
) -> Result<Expr, PlannerError> {
    expression
        .transform_up(|expr| match expr {
            Expr::Column(column) => {
                let output_idx = resolve_join_output_column_index(&column, join, relation_sides)?;
                let field = join.output_schema.field(output_idx).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "join output column index {output_idx} out of bounds",
                    ))
                })?;
                Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                    field.name.clone(),
                ))))
            }
            other => Ok(Transformed::no(other)),
        })
        .map(|result| result.data)
        .map_err(|err| PlannerError::AnalysisError(err.into()))
}

fn resolve_join_output_column_index(
    column: &Column,
    join: &DbspJoinNode,
    relation_sides: &HashMap<String, JoinInputSide>,
) -> Result<usize, DataFusionError> {
    if let Some(relation) = column.relation.as_ref().map(ToString::to_string)
        && let Some(side) = relation_sides.get(&relation)
    {
        return match side {
            JoinInputSide::Left => join
                .left_schema
                .field_index(column.name.as_str())
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "column '{}.{}' not found in left join input schema",
                        relation, column.name
                    ))
                }),
            JoinInputSide::Right => join
                .right_schema
                .field_index(column.name.as_str())
                .map(|idx| join.left_schema.len() + idx)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "column '{}.{}' not found in right join input schema",
                        relation, column.name
                    ))
                }),
        };
    }

    if let Some(output_idx) = join.output_schema.field_index(column.name.as_str()) {
        return Ok(output_idx);
    }

    match (
        join.left_schema.field_index(column.name.as_str()),
        join.right_schema.field_index(column.name.as_str()),
    ) {
        (Some(output_idx), None) => Ok(output_idx),
        (None, Some(right_idx)) => Ok(join.left_schema.len() + right_idx),
        _ => Err(DataFusionError::Plan(format!(
            "column '{}' could not be resolved in join output schema",
            column.flat_name()
        ))),
    }
}

fn split_join_filter(
    predicate: Option<&Expr>,
    output_schema: &RowSchema,
    left_width: usize,
    join: &DbspJoinNode,
) -> Result<SplitJoinFilter, PlannerError> {
    let Some(predicate) = predicate else {
        return Ok(SplitJoinFilter::default());
    };
    let normalized = normalize_expr(predicate.clone())?;
    let conjuncts = split_conjuncts(&normalized);
    let mut left_pushdown = Vec::new();
    let mut right_pushdown = Vec::new();
    let mut remaining = Vec::new();
    let mut required_columns = BTreeSet::new();
    let left_to_right_keys = join_key_column_mapping(join, JoinInputSide::Left);
    let right_to_left_keys = join_key_column_mapping(join, JoinInputSide::Right);

    for conjunct in conjuncts {
        let columns = expression_output_columns(&conjunct, output_schema)?;
        required_columns.extend(columns.iter().copied());
        let references_left = columns.iter().any(|column_idx| *column_idx < left_width);
        let references_right = columns.iter().any(|column_idx| *column_idx >= left_width);
        match (references_left, references_right) {
            (true, false) => {
                let left_predicate =
                    rewrite_join_output_expr_for_side(conjunct, join, JoinInputSide::Left)?;
                if let Some(right_predicate) =
                    rewrite_key_predicate_for_opposite_side(&left_predicate, &left_to_right_keys)?
                {
                    right_pushdown.push(right_predicate);
                }
                left_pushdown.push(left_predicate);
            }
            (false, true) => {
                let right_predicate =
                    rewrite_join_output_expr_for_side(conjunct, join, JoinInputSide::Right)?;
                if let Some(left_predicate) =
                    rewrite_key_predicate_for_opposite_side(&right_predicate, &right_to_left_keys)?
                {
                    left_pushdown.push(left_predicate);
                }
                right_pushdown.push(right_predicate);
            }
            _ => remaining.push(conjunct),
        }
    }

    Ok(SplitJoinFilter {
        left_pushdown: combine_filters(left_pushdown),
        right_pushdown: combine_filters(right_pushdown),
        remaining: combine_filters(remaining),
        required_columns,
    })
}

fn split_conjuncts(predicate: &Expr) -> Vec<Expr> {
    match predicate {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            let mut conjuncts = split_conjuncts(binary.left.as_ref());
            conjuncts.extend(split_conjuncts(binary.right.as_ref()));
            conjuncts
        }
        _ => vec![predicate.clone()],
    }
}

fn expression_output_columns(
    expression: &Expr,
    output_schema: &RowSchema,
) -> Result<BTreeSet<usize>, PlannerError> {
    let mut columns = BTreeSet::new();
    add_required_expression_columns(expression, output_schema, &mut columns)?;
    Ok(columns)
}

fn rewrite_join_output_expr_for_side(
    expression: Expr,
    join: &DbspJoinNode,
    side: JoinInputSide,
) -> Result<Expr, PlannerError> {
    expression
        .transform_up(|expr| match expr {
            Expr::Column(column) => {
                let output_idx = join
                    .output_schema
                    .field_index(column.name.as_str())
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "column '{}' not found in join output schema",
                            column.name
                        ))
                    })?;
                let rewritten = match side {
                    JoinInputSide::Left if output_idx < join.left_schema.len() => {
                        let field = join.left_schema.field(output_idx).ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "left join output column index {output_idx} out of bounds",
                            ))
                        })?;
                        Expr::Column(Column::new_unqualified(field.name.clone()))
                    }
                    JoinInputSide::Right if output_idx >= join.left_schema.len() => {
                        let right_idx = output_idx - join.left_schema.len();
                        let field = join.right_schema.field(right_idx).ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "right join output column index {right_idx} out of bounds",
                            ))
                        })?;
                        Expr::Column(Column::new_unqualified(field.name.clone()))
                    }
                    JoinInputSide::Left => {
                        return Err(DataFusionError::Plan(format!(
                            "attempted to rewrite right-side join column '{}' onto left input",
                            column.name
                        )));
                    }
                    JoinInputSide::Right => {
                        return Err(DataFusionError::Plan(format!(
                            "attempted to rewrite left-side join column '{}' onto right input",
                            column.name
                        )));
                    }
                };
                Ok(Transformed::yes(rewritten))
            }
            other => Ok(Transformed::no(other)),
        })
        .map(|result| result.data)
        .map_err(|err| PlannerError::AnalysisError(err.into()))
}

fn join_key_column_mapping(
    join: &DbspJoinNode,
    from_side: JoinInputSide,
) -> HashMap<String, String> {
    let mut mapping = HashMap::new();
    for key in &join.keys {
        let (from, to) = match from_side {
            JoinInputSide::Left => (key.left_expression().expr(), key.right_expression().expr()),
            JoinInputSide::Right => (key.right_expression().expr(), key.left_expression().expr()),
        };
        let (Expr::Column(from), Expr::Column(to)) = (from, to) else {
            continue;
        };
        mapping.insert(from.name.clone(), to.name.clone());
    }
    mapping
}

fn rewrite_key_predicate_for_opposite_side(
    predicate: &Expr,
    key_mapping: &HashMap<String, String>,
) -> Result<Option<Expr>, PlannerError> {
    if predicate.is_volatile() || key_mapping.is_empty() {
        return Ok(None);
    }

    let mut saw_key_column = false;
    let mut can_rewrite = true;
    let rewritten = predicate
        .clone()
        .transform_up(|expr| match expr {
            Expr::Column(column) => {
                let Some(target_name) = key_mapping.get(&column.name) else {
                    can_rewrite = false;
                    return Ok(Transformed::no(Expr::Column(column)));
                };
                saw_key_column = true;
                Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                    target_name.clone(),
                ))))
            }
            other => Ok(Transformed::no(other)),
        })
        .map(|result| result.data)
        .map_err(|err| PlannerError::AnalysisError(err.into()))?;

    Ok((can_rewrite && saw_key_column).then_some(rewritten))
}

fn add_required_expression_columns(
    expression: &Expr,
    input_schema: &RowSchema,
    columns: &mut BTreeSet<usize>,
) -> Result<(), PlannerError> {
    for column in expression.column_refs() {
        let column_idx = input_schema
            .field_index(column.name.as_str())
            .ok_or_else(|| {
                PlannerError::AnalysisError(anyhow::anyhow!(
                    "column '{}' not found in input schema",
                    column.name
                ))
            })?;
        columns.insert(column_idx);
    }
    Ok(())
}

fn split_join_required_columns(
    columns: &BTreeSet<usize>,
    left_width: usize,
    left_columns: &mut BTreeSet<usize>,
    right_columns: &mut BTreeSet<usize>,
) -> Result<(), PlannerError> {
    for column_idx in columns {
        if *column_idx < left_width {
            left_columns.insert(*column_idx);
        } else {
            let right_idx = column_idx.checked_sub(left_width).ok_or_else(|| {
                PlannerError::AnalysisError(anyhow::anyhow!(
                    "join column index underflow for {column_idx}",
                ))
            })?;
            right_columns.insert(right_idx);
        }
    }
    Ok(())
}

fn extract_row_number_limit(predicate: &Expr) -> Result<Option<(String, usize)>, PlannerError> {
    let normalized = normalize_expr(predicate.clone())?;
    let Expr::BinaryExpr(binary) = normalized else {
        return Ok(None);
    };

    let (column, literal, exclusive) = match (&*binary.left, binary.op, &*binary.right) {
        (Expr::Column(column), Operator::LtEq, literal @ Expr::Literal(_, _)) => {
            (column.name.clone(), literal, false)
        }
        (Expr::Column(column), Operator::Lt, literal @ Expr::Literal(_, _)) => {
            (column.name.clone(), literal, true)
        }
        _ => return Ok(None),
    };

    let mut limit = literal_to_positive_usize(literal)?;
    if exclusive {
        if limit == 0 {
            return Ok(None);
        }
        limit -= 1;
    }
    if limit == 0 {
        return Ok(None);
    }
    Ok(Some((column, limit)))
}

fn projection_expr_matches_rank(expr: &Expr, rank_column: &str) -> bool {
    match expr {
        Expr::Column(column) => column.name == rank_column,
        Expr::Alias(alias) => alias.name == rank_column,
        _ => false,
    }
}

fn literal_to_positive_usize(expr: &Expr) -> Result<usize, PlannerError> {
    let Expr::Literal(value, _) = expr else {
        return Err(PlannerError::UnsupportedPlan(
            "ROW_NUMBER filter limit must be a positive integer literal".to_string(),
        ));
    };
    let array = value.to_array().map_err(|_| {
        PlannerError::UnsupportedPlan(
            "ROW_NUMBER filter limit must be a positive integer literal".to_string(),
        )
    })?;
    let as_i128 = array_to_i128(array.as_ref()).ok_or_else(|| {
        PlannerError::UnsupportedPlan(
            "ROW_NUMBER filter limit must be a positive integer literal".to_string(),
        )
    })?;

    if as_i128 <= 0 {
        return Err(PlannerError::UnsupportedPlan(
            "ROW_NUMBER filter limit must be positive".to_string(),
        ));
    }
    usize::try_from(as_i128).map_err(|_| {
        PlannerError::UnsupportedPlan("ROW_NUMBER filter limit is out of range".to_string())
    })
}

fn array_to_i128(array: &dyn Array) -> Option<i128> {
    if let Some(values) = array.as_any().downcast_ref::<Int8Array>() {
        return (!values.is_null(0)).then(|| i128::from(values.value(0)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int16Array>() {
        return (!values.is_null(0)).then(|| i128::from(values.value(0)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        return (!values.is_null(0)).then(|| i128::from(values.value(0)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return (!values.is_null(0)).then(|| i128::from(values.value(0)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt8Array>() {
        return (!values.is_null(0)).then(|| i128::from(values.value(0)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt16Array>() {
        return (!values.is_null(0)).then(|| i128::from(values.value(0)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
        return (!values.is_null(0)).then(|| i128::from(values.value(0)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return (!values.is_null(0)).then(|| i128::from(values.value(0)));
    }
    None
}
