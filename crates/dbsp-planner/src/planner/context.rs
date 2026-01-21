use std::sync::Arc;

use datafusion::logical_expr::expr::Sort as ExprSort;
use datafusion::logical_expr::logical_plan::{FetchType, SkipType};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan};

use dbsp_circuit::circuit::plan::{
    DbspAggregateNode, DbspJoinNode, DbspJoinType, DbspNodeKind, DbspProjectNode, DbspSelectNode,
    DbspSourceNode, DbspTopNNode, DbspUnionNode, OrderExpr, ProjectItem,
};
use dbsp_circuit::circuit::schema::RowSchema;

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

                if scan.projection.is_some() {
                    return Err(PlannerError::UnsupportedPlan(
                        "column projection pushdown is not supported yet".to_string(),
                    ));
                }

                if !scan.filters.is_empty() {
                    return Err(PlannerError::UnsupportedPlan(
                        "table scan filters are not supported yet".to_string(),
                    ));
                }

                let node = DbspNodeKind::Source(DbspSourceNode { table });
                let schema = table.schema().clone();
                let id = self.add_node(vec![], node, schema.clone());
                Ok(PlannedNode { id, schema })
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
            LogicalPlan::Distinct(_) => Err(PlannerError::UnsupportedPlan(
                "DISTINCT is not supported".to_string(),
            )),
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
                match (&left, &right) {
                    (Expr::Column(_), Expr::Column(_)) => Ok((left, right)),
                    _ => Err(PlannerError::UnsupportedJoin(
                        "join keys must be column references".to_string(),
                    )),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

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

        let group_keys = aggregate
            .group_expr
            .iter()
            .map(|expr| extract_alias(expr.clone()))
            .collect::<Result<Vec<_>, _>>()?;

        let aggregates = aggregate
            .aggr_expr
            .iter()
            .map(map_aggregate_expr)
            .collect::<Result<Vec<_>, _>>()?;

        let agg_node = DbspAggregateNode::try_new(input.schema.clone(), group_keys, aggregates)?;
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
