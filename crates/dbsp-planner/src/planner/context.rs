use std::sync::Arc;

use datafusion::logical_expr::expr::Sort as ExprSort;
use datafusion::logical_expr::logical_plan::{FetchType, SkipType};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, Operator, WindowFunctionDefinition};
use datafusion::scalar::ScalarValue;
use datafusion_common::Column;

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

pub struct CircuitPlanner {
    config: PlannerConfig,
}

impl CircuitPlanner {
    pub fn new(config: PlannerConfig) -> Self {
        Self { config }
    }

    pub fn plan(&self, plan: &LogicalPlan) -> Result<CircuitPlan, PlannerError> {
        let mut ctx = PlannerContext::new(&self.config);
        let planned = ctx.plan_node(plan)?;
        Ok(CircuitPlan {
            root: planned.id,
            nodes: ctx.into_nodes(),
        })
    }
}

struct PlannerContext<'cfg> {
    config: &'cfg PlannerConfig,
    nodes: Vec<CircuitNode>,
}

struct PlannedNode {
    id: usize,
    schema: Arc<RowSchema>,
}

type RowNumberSpec = (String, Vec<Expr>, Vec<ExprSort>);

impl<'cfg> PlannerContext<'cfg> {
    fn new(config: &'cfg PlannerConfig) -> Self {
        Self {
            config,
            nodes: Vec::new(),
        }
    }

    fn into_nodes(self) -> Vec<CircuitNode> {
        self.nodes
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
                    DbspNodeKind::Source(DbspSourceNode { table }),
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
                if let LogicalPlan::Projection(projection) = filter.input.as_ref() {
                    let base = self.plan_node(&projection.input)?;
                    let select = DbspSelectNode::try_new(
                        base.schema.clone(),
                        normalize_expr(filter.predicate.clone())?,
                    )?;
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

                let input = self.plan_node(&filter.input)?;
                let mut predicate_schema = input.schema.clone();
                if matches!(filter.input.as_ref(), LogicalPlan::Projection(_))
                    && let Some(node) = self.node_by_id(input.id)
                    && let DbspNodeKind::Project(project) = &node.kind
                {
                    predicate_schema = Arc::clone(project.input_schema());
                }
                let select = DbspSelectNode::try_new(
                    predicate_schema,
                    normalize_expr(filter.predicate.clone())?,
                )?;
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
        self.build_projection_items(input, &projection.expr)
    }

    fn build_projection_items(
        &mut self,
        input: PlannedNode,
        expressions: &[Expr],
    ) -> Result<PlannedNode, PlannerError> {
        let items = expressions
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
                    0
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
                    0
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
                    0
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
        match expr {
            Expr::Literal(ScalarValue::Int64(Some(value)), _) => Ok(*value),
            _ => Err(PlannerError::UnsupportedPlan(
                "window sizes must be literal Int64 milliseconds".to_string(),
            )),
        }
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

fn extract_row_number_limit(predicate: &Expr) -> Result<Option<(String, usize)>, PlannerError> {
    let normalized = normalize_expr(predicate.clone())?;
    let Expr::BinaryExpr(binary) = normalized else {
        return Ok(None);
    };

    let (column, literal, exclusive) = match (&*binary.left, binary.op, &*binary.right) {
        (Expr::Column(column), Operator::LtEq, Expr::Literal(value, _)) => {
            (column.name.clone(), value.clone(), false)
        }
        (Expr::Column(column), Operator::Lt, Expr::Literal(value, _)) => {
            (column.name.clone(), value.clone(), true)
        }
        _ => return Ok(None),
    };

    let mut limit = scalar_to_positive_usize(&literal)?;
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

fn scalar_to_positive_usize(value: &ScalarValue) -> Result<usize, PlannerError> {
    let as_i128 = match value {
        ScalarValue::Int8(Some(v)) => i128::from(*v),
        ScalarValue::Int16(Some(v)) => i128::from(*v),
        ScalarValue::Int32(Some(v)) => i128::from(*v),
        ScalarValue::Int64(Some(v)) => i128::from(*v),
        ScalarValue::UInt8(Some(v)) => i128::from(*v),
        ScalarValue::UInt16(Some(v)) => i128::from(*v),
        ScalarValue::UInt32(Some(v)) => i128::from(*v),
        ScalarValue::UInt64(Some(v)) => i128::from(*v),
        _ => {
            return Err(PlannerError::UnsupportedPlan(
                "ROW_NUMBER filter limit must be a positive integer literal".to_string(),
            ));
        }
    };

    if as_i128 <= 0 {
        return Err(PlannerError::UnsupportedPlan(
            "ROW_NUMBER filter limit must be positive".to_string(),
        ));
    }
    usize::try_from(as_i128).map_err(|_| {
        PlannerError::UnsupportedPlan("ROW_NUMBER filter limit is out of range".to_string())
    })
}
