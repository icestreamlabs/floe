use std::collections::HashMap;
use std::fmt::{self, Display};
use std::sync::Arc;

use anyhow::Error as AnyError;
use datafusion::common::TableReference;
use datafusion::logical_expr::expr::Sort as ExprSort;
use datafusion::logical_expr::logical_plan::{FetchType, SkipType};
use datafusion::logical_expr::{BinaryExpr, Expr, JoinType, LogicalPlan, Operator};
use datafusion_common::{
    Column,
    tree_node::{Transformed, TreeNode},
};

use crate::circuit::plan::{
    DbspAggregateFunction, DbspAggregateNode, DbspJoinNode, DbspJoinType, DbspNodeKind,
    DbspProjectNode, DbspSelectNode, DbspSourceNode, DbspTopNNode, DbspUnionNode, OrderExpr,
    ProjectItem,
};
use crate::circuit::schema::RowSchema;
use crate::circuit::tables::TableDescriptor;

#[derive(Debug, Clone)]
pub struct PlannerConfig {
    tables: HashMap<String, &'static TableDescriptor>,
}

impl PlannerConfig {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn with_table(mut self, table: &'static TableDescriptor) -> Self {
        self.register_table(table);
        self
    }

    pub fn register_table(&mut self, table: &'static TableDescriptor) {
        self.tables.insert(table.name.to_string(), table);
    }

    pub fn register_alias(&mut self, alias: &str, table: &'static TableDescriptor) {
        self.tables.insert(alias.to_string(), table);
    }

    fn table(&self, name: &TableReference) -> Option<&'static TableDescriptor> {
        self.tables
            .get(name.table())
            .copied()
            .or_else(|| self.tables.get(&name.to_string()).copied())
    }
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct CircuitNode {
    pub id: usize,
    pub kind: DbspNodeKind,
    pub inputs: Vec<usize>,
    pub output_schema: Arc<RowSchema>,
}

#[derive(Debug, Clone)]
pub struct CircuitPlan {
    pub root: usize,
    pub nodes: Vec<CircuitNode>,
}

impl CircuitPlan {
    pub fn nodes(&self) -> &[CircuitNode] {
        &self.nodes
    }

    pub fn node(&self, id: usize) -> Option<&CircuitNode> {
        self.nodes.iter().find(|node| node.id == id)
    }
}

#[derive(Debug)]
pub enum PlannerError {
    TableNotFound(String),
    UnsupportedPlan(String),
    UnsupportedJoin(String),
    UnsupportedExpression(String),
    AnalysisError(AnyError),
}

impl Display for PlannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlannerError::TableNotFound(name) => {
                write!(f, "table '{name}' is not registered in the planner")
            }
            PlannerError::UnsupportedPlan(desc) => write!(f, "unsupported logical plan: {desc}"),
            PlannerError::UnsupportedJoin(desc) => write!(f, "unsupported join: {desc}"),
            PlannerError::UnsupportedExpression(desc) => {
                write!(f, "unsupported expression: {desc}")
            }
            PlannerError::AnalysisError(err) => write!(f, "expression analysis failed: {err}"),
        }
    }
}

impl std::error::Error for PlannerError {}

impl From<AnyError> for PlannerError {
    fn from(err: AnyError) -> Self {
        PlannerError::AnalysisError(err)
    }
}

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
                if matches!(filter.input.as_ref(), LogicalPlan::Projection(_)) {
                    if let Some(node) = self.node_by_id(input.id) {
                        if let DbspNodeKind::Project(project) = &node.kind {
                            predicate_schema = Arc::clone(project.input_schema());
                        }
                    }
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
            .map(|expr| map_aggregate_expr(expr))
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

fn normalize_expr(expr: Expr) -> Result<Expr, PlannerError> {
    expr.transform_up(|expr| match expr {
        Expr::Column(column) => Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
            column.name.clone(),
        )))),
        Expr::OuterReferenceColumn(data_type, column) => Ok(Transformed::yes(
            Expr::OuterReferenceColumn(data_type, Column::new_unqualified(column.name.clone())),
        )),
        other => Ok(Transformed::no(other)),
    })
    .map(|result| result.data)
    .map_err(|err| PlannerError::AnalysisError(err.into()))
}

fn combine_filters(filters: Vec<Expr>) -> Option<Expr> {
    let mut iter = filters.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, expr| {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(acc),
            op: Operator::And,
            right: Box::new(expr),
        })
    }))
}

fn extract_join_keys_and_residual(
    expr: &Expr,
) -> Result<(Vec<(Expr, Expr)>, Option<Expr>), PlannerError> {
    let mut key_pairs = Vec::new();
    let mut residuals = Vec::new();
    accumulate_conjuncts(expr, &mut key_pairs, &mut residuals)?;
    Ok((key_pairs, combine_filters(residuals)))
}

fn accumulate_conjuncts(
    expr: &Expr,
    key_pairs: &mut Vec<(Expr, Expr)>,
    residuals: &mut Vec<Expr>,
) -> Result<(), PlannerError> {
    let normalized = normalize_expr(expr.clone())?;
    match &normalized {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            accumulate_conjuncts(&binary.left, key_pairs, residuals)?;
            accumulate_conjuncts(&binary.right, key_pairs, residuals)?;
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            match (&*binary.left, &*binary.right) {
                (Expr::Column(_), Expr::Column(_)) => {
                    key_pairs.push(((*binary.left).clone(), (*binary.right).clone()))
                }
                _ => residuals.push(normalized),
            }
        }
        _ => residuals.push(normalized),
    }
    Ok(())
}

fn extract_alias(expr: Expr) -> Result<(Expr, Option<String>), PlannerError> {
    if let Expr::Alias(alias) = expr {
        let (inner, existing_alias) = extract_alias(alias.expr.as_ref().clone())?;
        let alias = existing_alias.or_else(|| Some(alias.name.clone()));
        Ok((inner, alias))
    } else {
        Ok((normalize_expr(expr)?, None))
    }
}

#[allow(deprecated)]
fn is_wildcard_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::Wildcard { .. })
}

fn map_aggregate_expr(
    expr: &Expr,
) -> Result<(DbspAggregateFunction, Option<Expr>, Option<String>), PlannerError> {
    match expr {
        Expr::Alias(alias) => {
            let (function, arg, existing_alias) = map_aggregate_expr(alias.expr.as_ref())?;
            let alias = existing_alias.or_else(|| Some(alias.name.clone()));
            Ok((function, arg, alias))
        }
        Expr::AggregateFunction(func) => {
            if func.params.distinct {
                return Err(PlannerError::UnsupportedPlan(
                    "DISTINCT aggregates are not supported".to_string(),
                ));
            }
            if func.params.filter.is_some() {
                return Err(PlannerError::UnsupportedPlan(
                    "FILTER clauses on aggregates are not supported".to_string(),
                ));
            }
            if !func.params.order_by.is_empty() {
                return Err(PlannerError::UnsupportedPlan(
                    "ORDER BY within aggregates is not supported".to_string(),
                ));
            }
            if func.params.null_treatment.is_some() {
                return Err(PlannerError::UnsupportedPlan(
                    "NULL treatment modifiers on aggregates are not supported".to_string(),
                ));
            }

            let name = func.func.name().to_ascii_lowercase();
            let agg_function = match name.as_str() {
                "count" => DbspAggregateFunction::Count,
                "sum" => DbspAggregateFunction::Sum,
                "min" => DbspAggregateFunction::Min,
                "max" => DbspAggregateFunction::Max,
                "avg" => DbspAggregateFunction::Avg,
                other => {
                    return Err(PlannerError::UnsupportedPlan(format!(
                        "aggregate function '{other}' is not supported",
                    )));
                }
            };

            if func.params.args.len() > 1 {
                return Err(PlannerError::UnsupportedPlan(
                    "aggregates with more than one argument are not supported".to_string(),
                ));
            }

            let expression = func
                .params
                .args
                .first()
                .and_then(|arg| (!is_wildcard_expr(arg)).then(|| arg.clone()));

            let expression = match expression {
                Some(expr) => Some(normalize_expr(expr)?),
                None => None,
            };

            Ok((agg_function, expression, None))
        }
        _ => Err(PlannerError::UnsupportedPlan(
            "aggregate expressions must be aggregate functions".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::functions_aggregate::expr_fn::{avg, count, sum};
    use datafusion::logical_expr::expr::WildcardOptions;
    use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;
    use datafusion::logical_expr::{JoinType, LogicalPlanBuilder, TableSource, col};

    fn planner_config() -> PlannerConfig {
        let mut config = PlannerConfig::new();
        config.register_table(crate::circuit::tables::nexmark_person_table());
        config.register_table(crate::circuit::tables::nexmark_auction_table());
        config.register_table(crate::circuit::tables::nexmark_bid_table());
        config
    }

    fn table_source(table: &'static TableDescriptor) -> Arc<dyn TableSource> {
        Arc::new(LogicalTableSource::new(table.schema().to_arrow_schema()))
    }

    fn qualified(table: &'static TableDescriptor, column: &str) -> String {
        format!("{}.{}", table.name, column)
    }

    #[test]
    fn count_star_maps_to_untyped_count() {
        #[allow(deprecated)]
        let wildcard = Expr::Wildcard {
            qualifier: None,
            options: Box::<WildcardOptions>::default(),
        };

        let expr = count(wildcard);
        let (function, arg, alias) = map_aggregate_expr(&expr).expect("map aggregate");

        assert!(matches!(function, DbspAggregateFunction::Count));
        assert!(arg.is_none());
        assert!(alias.is_none());
    }

    #[test]
    fn plans_projection_over_scan() {
        let table = crate::circuit::tables::nexmark_person_table();
        let plan = LogicalPlanBuilder::scan(table.name, table_source(table), None)
            .unwrap()
            .project(vec![
                col(qualified(table, "id")),
                col(qualified(table, "name")),
            ])
            .unwrap()
            .build()
            .unwrap();

        let planner = CircuitPlanner::new(planner_config());
        let circuit_plan = planner.plan(&plan).expect("plan");
        let root = circuit_plan.node(circuit_plan.root).unwrap();
        match &root.kind {
            DbspNodeKind::Project(project) => {
                assert_eq!(project.output_schema().len(), 2);
            }
            other => panic!("expected project node, found {other:?}"),
        }
    }

    #[test]
    fn plans_inner_join() {
        let person = crate::circuit::tables::nexmark_person_table();
        let auction = crate::circuit::tables::nexmark_auction_table();

        let left = LogicalPlanBuilder::scan(person.name, table_source(person), None)
            .unwrap()
            .build()
            .unwrap();
        let right = LogicalPlanBuilder::scan(auction.name, table_source(auction), None)
            .unwrap()
            .build()
            .unwrap();

        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (
                    vec![qualified(person, "id")],
                    vec![qualified(auction, "seller")],
                ),
                None,
            )
            .unwrap()
            .build()
            .unwrap();

        let planner = CircuitPlanner::new(planner_config());
        let circuit_plan = planner.plan(&plan).expect("plan");
        let root = circuit_plan.node(circuit_plan.root).unwrap();
        match &root.kind {
            DbspNodeKind::Join(join) => {
                assert_eq!(join.keys.len(), 1);
            }
            other => panic!("expected join node, found {other:?}"),
        }
    }

    #[test]
    fn plans_aggregate_and_topn() {
        let bid = crate::circuit::tables::nexmark_bid_table();

        let plan = LogicalPlanBuilder::scan(bid.name, table_source(bid), None)
            .unwrap()
            .aggregate(
                vec![col(qualified(bid, "bidder"))],
                vec![
                    sum(col(qualified(bid, "price"))).alias("total_price"),
                    count(col(qualified(bid, "price"))).alias("bid_count"),
                    avg(col(qualified(bid, "price"))).alias("avg_price"),
                ],
            )
            .unwrap()
            .sort(vec![col("total_price").sort(true, true)])
            .unwrap()
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let planner = CircuitPlanner::new(planner_config());
        let circuit_plan = planner.plan(&plan).expect("plan");
        let root = circuit_plan.node(circuit_plan.root).unwrap();
        match &root.kind {
            DbspNodeKind::TopN(topn) => {
                assert_eq!(topn.output_schema().len(), 4);
            }
            other => panic!("expected TopN node, found {other:?}"),
        }
    }
}
