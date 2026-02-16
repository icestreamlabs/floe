use std::sync::Arc;

use datafusion::logical_expr::expr::Sort as ExprSort;
use datafusion::logical_expr::logical_plan::{FetchType, SkipType};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan};
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
        let items = projection
            .expr
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

    fn plan_join(
        &mut self,
        join: &datafusion::logical_expr::logical_plan::Join,
    ) -> Result<PlannedNode, PlannerError> {
        if join.join_type != JoinType::Inner {
            return Err(PlannerError::UnsupportedJoin(format!(
                "only inner joins are supported (found {:?})",
                join.join_type
            )));
        }

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

        let join_node = DbspJoinNode::try_new(
            DbspJoinType::Inner,
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
            group_keys.push(extract_alias(expr.clone())?);
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
            let topn = DbspTopNNode::try_new(input.schema.clone(), order_by, fetch, offset)?;
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
                if func.args.len() != 2 {
                    return Err(PlannerError::UnsupportedPlan(
                        "TUMBLE requires (time_expr, size_ms) arguments".to_string(),
                    ));
                }
                let time_expr = normalize_expr(func.args[0].clone())?;
                let size_ms = self.parse_window_arg(&func.args[1])?;
                let spec = DbspWindowSpec::try_new(
                    DbspWindowPolicy::Tumbling { size_ms },
                    time_expr,
                    input_schema,
                    0,
                )?;
                Ok(Some(spec))
            }
            "hop" => {
                if func.args.len() != 3 {
                    return Err(PlannerError::UnsupportedPlan(
                        "HOP requires (time_expr, slide_ms, size_ms) arguments".to_string(),
                    ));
                }
                let time_expr = normalize_expr(func.args[0].clone())?;
                let slide_ms = self.parse_window_arg(&func.args[1])?;
                let size_ms = self.parse_window_arg(&func.args[2])?;
                let spec = DbspWindowSpec::try_new(
                    DbspWindowPolicy::Hopping { size_ms, slide_ms },
                    time_expr,
                    input_schema,
                    0,
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
