use anyhow::{Context, Result, bail};
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{
    BinaryExpr, Expr as DFExpr, Filter, Join, JoinConstraint, JoinType, LogicalPlan,
    Operator as DFOperator, Projection, TableScan,
};
use datafusion::scalar::ScalarValue;

use crate::dataflow_plan::{
    DataflowPlan, Expr, FilterNode, JoinNode, MapNode, MaterializeNode, OperatorNode, ScanNode,
};
use crate::stream_types::{OperatorId, OutputPort};

pub struct QueryPlanner;

impl QueryPlanner {
    pub fn new() -> Self {
        Self
    }

    pub fn plan(
        &self,
        logical_plan: &LogicalPlan,
        view_name: impl Into<String>,
    ) -> Result<DataflowPlan> {
        let view_name = view_name.into();
        let mut builder = PlanBuilder::new(view_name.clone());
        let planned_stream = builder.plan_node(logical_plan)?;
        let materialize_id = builder.add_materialize(planned_stream.output, view_name);
        builder.plan.set_root(materialize_id);
        Ok(builder.plan)
    }
}

struct PlanBuilder {
    plan: DataflowPlan,
}

struct PlannedStream {
    output: OutputPort,
    schema: DFSchemaRef,
}

impl PlanBuilder {
    fn new(graph_id: impl Into<String>) -> Self {
        Self {
            plan: DataflowPlan::new(graph_id),
        }
    }

    fn plan_node(&mut self, plan: &LogicalPlan) -> Result<PlannedStream> {
        match plan {
            LogicalPlan::TableScan(scan) => self.plan_scan(scan),
            LogicalPlan::Projection(projection) => self.plan_projection(projection),
            LogicalPlan::Filter(filter) => self.plan_filter(filter),
            LogicalPlan::Join(join) => self.plan_join(join),
            LogicalPlan::SubqueryAlias(alias) => self.plan_node(alias.input.as_ref()),
            other => bail!("Unsupported logical plan node: {}", other.display_indent()),
        }
    }

    fn plan_scan(&mut self, scan: &TableScan) -> Result<PlannedStream> {
        let node = OperatorNode::Scan(ScanNode {
            source_name: scan.table_name.to_string(),
            output: OutputPort::new(OperatorId(usize::MAX), 0),
        });
        let operator_id = self.plan.add_operator(node);
        let output = self.assign_output(operator_id);
        Ok(PlannedStream {
            output,
            schema: scan.projected_schema.clone(),
        })
    }

    fn plan_projection(&mut self, projection: &Projection) -> Result<PlannedStream> {
        let input = self.plan_node(projection.input.as_ref())?;
        let mut expressions = Vec::with_capacity(projection.expr.len());
        for expr in &projection.expr {
            expressions.push(self.convert_expr(expr, &input.schema)?);
        }

        let node = OperatorNode::Map(MapNode {
            input: input.output,
            output: OutputPort::new(OperatorId(usize::MAX), 0),
            expressions,
        });
        let operator_id = self.plan.add_operator(node);
        let output = self.assign_output(operator_id);
        Ok(PlannedStream {
            output,
            schema: projection.schema.clone(),
        })
    }

    fn plan_filter(&mut self, filter: &Filter) -> Result<PlannedStream> {
        let input = self.plan_node(filter.input.as_ref())?;
        let predicate = self.convert_expr(&filter.predicate, &input.schema)?;
        let node = OperatorNode::Filter(FilterNode {
            input: input.output,
            output: OutputPort::new(OperatorId(usize::MAX), 0),
            predicate,
        });
        let operator_id = self.plan.add_operator(node);
        let output = self.assign_output(operator_id);
        Ok(PlannedStream {
            output,
            schema: input.schema,
        })
    }

    fn plan_join(&mut self, join: &Join) -> Result<PlannedStream> {
        if join.join_type != JoinType::Inner {
            bail!("only inner joins are supported in phase 2");
        }
        if join.filter.is_some() {
            bail!("non-equi join filters are not supported");
        }
        if join.join_constraint != JoinConstraint::On {
            bail!("only ON joins are supported");
        }

        let left = self.plan_node(join.left.as_ref())?;
        let right = self.plan_node(join.right.as_ref())?;
        let on = self.build_join_keys(&join.on, &left.schema, &right.schema)?;
        let projection =
            self.identity_projection(left.schema.fields().len() + right.schema.fields().len());

        let node = OperatorNode::Join(JoinNode {
            left: left.output,
            right: right.output,
            output: OutputPort::new(OperatorId(usize::MAX), 0),
            on,
            projection,
        });
        let operator_id = self.plan.add_operator(node);
        let output = self.assign_output(operator_id);
        Ok(PlannedStream {
            output,
            schema: join.schema.clone(),
        })
    }

    fn add_materialize(&mut self, input: OutputPort, view_name: String) -> OperatorId {
        let node = OperatorNode::Materialize(MaterializeNode { input, view_name });
        self.plan.add_operator(node)
    }

    fn assign_output(&mut self, operator_id: OperatorId) -> OutputPort {
        let output = OutputPort::new(operator_id, 0);
        match self.plan.get_mut(operator_id).expect("operator must exist") {
            OperatorNode::Scan(node) => node.output = output,
            OperatorNode::Map(node) => node.output = output,
            OperatorNode::Filter(node) => node.output = output,
            OperatorNode::Join(node) => node.output = output,
            OperatorNode::Materialize(_) => {}
        }
        output
    }

    fn convert_expr(&self, expr: &DFExpr, schema: &DFSchemaRef) -> Result<Expr> {
        match expr {
            DFExpr::Alias(alias) => self.convert_expr(alias.expr.as_ref(), schema),
            DFExpr::Column(column) => {
                let index = schema
                    .index_of_column(column)
                    .with_context(|| format!("column {} not found", column.name))?;
                Ok(Expr::Column(index))
            }
            DFExpr::Literal(value, _) => Ok(Expr::Literal(value.clone())),
            DFExpr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let left_expr = self.convert_expr(left, schema)?;
                let right_expr = self.convert_expr(right, schema)?;
                let (left_expr, right_expr) = coerce_numeric_literals(left_expr, right_expr);
                match op {
                    DFOperator::Eq => Ok(Expr::Eq(Box::new(left_expr), Box::new(right_expr))),
                    DFOperator::NotEq => Ok(Expr::NotEq(Box::new(left_expr), Box::new(right_expr))),
                    DFOperator::Lt => Ok(Expr::Lt(Box::new(left_expr), Box::new(right_expr))),
                    DFOperator::LtEq => Ok(Expr::LtEq(Box::new(left_expr), Box::new(right_expr))),
                    DFOperator::Gt => Ok(Expr::Gt(Box::new(left_expr), Box::new(right_expr))),
                    DFOperator::GtEq => Ok(Expr::GtEq(Box::new(left_expr), Box::new(right_expr))),
                    DFOperator::And => Ok(Expr::And(Box::new(left_expr), Box::new(right_expr))),
                    DFOperator::Or => Ok(Expr::Or(Box::new(left_expr), Box::new(right_expr))),
                    DFOperator::Plus => Ok(Expr::Add(Box::new(left_expr), Box::new(right_expr))),
                    DFOperator::Minus => Ok(Expr::Sub(Box::new(left_expr), Box::new(right_expr))),
                    DFOperator::Multiply => {
                        Ok(Expr::Mul(Box::new(left_expr), Box::new(right_expr)))
                    }
                    DFOperator::Divide => Ok(Expr::Div(Box::new(left_expr), Box::new(right_expr))),
                    DFOperator::Modulo => Ok(Expr::Mod(Box::new(left_expr), Box::new(right_expr))),
                    _ => bail!("unsupported binary operator: {op:?} in expression {expr}"),
                }
            }
            DFExpr::Negative(inner) => {
                let child = self.convert_expr(inner, schema)?;
                Ok(Expr::Neg(Box::new(child)))
            }
            DFExpr::InList(InList {
                expr: needle,
                list,
                negated,
            }) => {
                if list.is_empty() {
                    bail!("IN() list must contain at least one value: {expr}");
                }
                let converted_expr = self.convert_expr(needle, schema)?;
                let mut converted_list = Vec::with_capacity(list.len());
                for value in list {
                    converted_list.push(self.convert_expr(value, schema)?);
                }
                Ok(Expr::InList {
                    expr: Box::new(converted_expr),
                    list: converted_list,
                    negated: *negated,
                })
            }
            other => bail!("unsupported expression in MVP: {other}"),
        }
    }

    fn build_join_keys(
        &self,
        on: &[(DFExpr, DFExpr)],
        left_schema: &DFSchemaRef,
        right_schema: &DFSchemaRef,
    ) -> Result<Vec<(usize, usize)>> {
        let mut keys = Vec::with_capacity(on.len());
        for (left_expr, right_expr) in on {
            let left_index = self.extract_column_index(left_expr, left_schema)?;
            let right_index = self.extract_column_index(right_expr, right_schema)?;
            keys.push((left_index, right_index));
        }
        Ok(keys)
    }

    fn extract_column_index(&self, expr: &DFExpr, schema: &DFSchemaRef) -> Result<usize> {
        match expr {
            DFExpr::Column(column) => schema
                .index_of_column(column)
                .with_context(|| format!("column {} not found in join input", column.name)),
            _ => bail!("join keys must be column references"),
        }
    }

    fn identity_projection(&self, field_count: usize) -> Vec<Expr> {
        (0..field_count).map(Expr::Column).collect()
    }
}

fn coerce_numeric_literals(left: Expr, right: Expr) -> (Expr, Expr) {
    if matches!(left, Expr::Literal(ScalarValue::Float64(_)))
        && matches!(right, Expr::Literal(ScalarValue::Int64(_)))
    {
        return (left, int_literal_to_float(right));
    }
    if matches!(right, Expr::Literal(ScalarValue::Float64(_)))
        && matches!(left, Expr::Literal(ScalarValue::Int64(_)))
    {
        return (int_literal_to_float(left), right);
    }
    (left, right)
}

fn int_literal_to_float(expr: Expr) -> Expr {
    match expr {
        Expr::Literal(ScalarValue::Int64(value)) => {
            Expr::Literal(ScalarValue::Float64(value.map(|v| v as f64)))
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{Result, bail};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::common::Column;
    use datafusion::logical_expr::{col, lit, table_scan};
    use datafusion::scalar::ScalarValue;

    fn plan_filter_expr(schema: &Schema, predicate: DFExpr) -> Result<Expr> {
        let logical_plan = table_scan(Some("auction"), schema, None)?
            .filter(predicate)?
            .build()?;
        let planner = QueryPlanner::new();
        let dataflow = planner.plan(&logical_plan, "mv_test")?;
        match &dataflow.operators[1] {
            OperatorNode::Filter(node) => Ok(node.predicate.clone()),
            other => bail!("expected filter operator, found {other:?}"),
        }
    }

    fn bid_schema() -> Schema {
        Schema::new(vec![
            Field::new("auction", DataType::Int64, false),
            Field::new("bidder", DataType::Int64, false),
            Field::new("price", DataType::Int64, false),
        ])
    }

    fn auction_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("seller", DataType::Int64, false),
            Field::new("category", DataType::Int64, false),
        ])
    }

    fn person_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ])
    }

    #[test]
    fn map_expression_eval() -> Result<()> {
        let schema = bid_schema();
        let logical_plan = table_scan(Some("bid"), &schema, None)?
            .project(vec![
                (col("auction") + lit(1i64)).alias("auction_plus_one"),
                col("price"),
            ])?
            .build()?;

        let planner = QueryPlanner::new();
        let dataflow = planner.plan(&logical_plan, "mv_map_test")?;

        assert_eq!(dataflow.operators.len(), 3);
        match &dataflow.operators[1] {
            OperatorNode::Map(map_node) => {
                assert_eq!(map_node.expressions.len(), 2);
                match &map_node.expressions[0] {
                    Expr::Add(lhs, rhs) => {
                        assert!(matches!(**lhs, Expr::Column(0)));
                        match rhs.as_ref() {
                            Expr::Literal(value) => {
                                assert_eq!(value, &ScalarValue::from(1i64));
                            }
                            other => panic!("expected literal, found {other:?}"),
                        }
                    }
                    other => panic!("unexpected expression: {other:?}"),
                }
                assert!(matches!(map_node.expressions[1], Expr::Column(2)));
            }
            other => panic!("expected map operator, found {other:?}"),
        }

        match &dataflow.operators[2] {
            OperatorNode::Materialize(node) => assert_eq!(node.view_name, "mv_map_test"),
            _ => panic!("expected terminal materialize"),
        }

        Ok(())
    }

    #[test]
    fn map_supports_extended_arithmetic() -> Result<()> {
        let schema = bid_schema();
        let logical_plan = table_scan(Some("bid"), &schema, None)?
            .project(vec![
                (col("price") - lit(1i64)).alias("price_minus_one"),
                (col("price") * lit(2i64)).alias("price_times_two"),
                (col("price") / lit(2i64)).alias("price_div_two"),
                (col("price") % lit(2i64)).alias("price_mod_two"),
                (-col("price")).alias("neg_price"),
            ])?
            .build()?;

        let planner = QueryPlanner::new();
        let dataflow = planner.plan(&logical_plan, "mv_map_extended")?;
        let map_node = match &dataflow.operators[1] {
            OperatorNode::Map(node) => node,
            other => bail!("expected map operator, found {other:?}"),
        };
        assert_eq!(map_node.expressions.len(), 5);
        assert!(matches!(map_node.expressions[0], Expr::Sub(_, _)));
        assert!(matches!(map_node.expressions[1], Expr::Mul(_, _)));
        assert!(matches!(map_node.expressions[2], Expr::Div(_, _)));
        assert!(matches!(map_node.expressions[3], Expr::Mod(_, _)));
        assert!(matches!(map_node.expressions[4], Expr::Neg(_)));
        Ok(())
    }

    #[test]
    fn filter_predicate() -> Result<()> {
        let schema = bid_schema();
        let predicate = col("bidder")
            .eq(lit(42i64))
            .and(col("auction").eq(lit(7i64)));
        let logical_plan = table_scan(Some("bid"), &schema, None)?
            .filter(predicate)?
            .build()?;

        let planner = QueryPlanner::new();
        let dataflow = planner.plan(&logical_plan, "mv_filter_test")?;

        match &dataflow.operators[1] {
            OperatorNode::Filter(filter_node) => match &filter_node.predicate {
                Expr::And(lhs, rhs) => {
                    assert!(matches!(**lhs, Expr::Eq(_, _)));
                    assert!(matches!(**rhs, Expr::Eq(_, _)));
                }
                other => panic!("unexpected predicate shape: {other:?}"),
            },
            _ => panic!("expected filter operator"),
        }

        Ok(())
    }

    #[test]
    fn filter_supports_extended_predicates() -> Result<()> {
        let schema = auction_schema();
        let modulo_expr = plan_filter_expr(&schema, (col("category") % lit(123i64)).eq(lit(0i64)))?;
        match modulo_expr {
            Expr::Eq(lhs, rhs) => {
                assert!(matches!(*lhs, Expr::Mod(_, _)));
                assert!(matches!(*rhs, Expr::Literal(_)));
            }
            other => panic!("expected equality predicate, found {other:?}"),
        }

        let in_list_expr = plan_filter_expr(
            &schema,
            col("category").in_list(vec![lit(10i64), lit(20i64)], false),
        )?;
        match in_list_expr {
            Expr::InList { list, negated, .. } => {
                assert_eq!(list.len(), 2);
                assert!(!negated);
            }
            other => panic!("expected IN predicate, found {other:?}"),
        }

        let gt_expr = plan_filter_expr(&schema, col("category").gt(lit(10i64)))?;
        assert!(matches!(gt_expr, Expr::Gt(_, _)));

        let gte_expr = plan_filter_expr(&schema, col("category").gt_eq(lit(10i64)))?;
        assert!(matches!(gte_expr, Expr::GtEq(_, _)));

        let lt_expr = plan_filter_expr(&schema, col("category").lt(lit(10i64)))?;
        assert!(matches!(lt_expr, Expr::Lt(_, _)));

        let lte_expr = plan_filter_expr(&schema, col("category").lt_eq(lit(10i64)))?;
        assert!(matches!(lte_expr, Expr::LtEq(_, _)));

        let not_eq_expr = plan_filter_expr(&schema, col("category").not_eq(lit(10i64)))?;
        assert!(matches!(not_eq_expr, Expr::NotEq(_, _)));

        Ok(())
    }

    #[test]
    fn coerces_int_literal_when_paired_with_float_literal() -> Result<()> {
        let schema = bid_schema();
        let logical_plan = table_scan(Some("bid"), &schema, None)?
            .project(vec![
                (lit(ScalarValue::Float64(Some(0.5))) + lit(ScalarValue::Int64(Some(2))))
                    .alias("mixed_literal"),
            ])?
            .build()?;

        let planner = QueryPlanner::new();
        let dataflow = planner.plan(&logical_plan, "mv_literal_test")?;
        let map_node = match &dataflow.operators[1] {
            OperatorNode::Map(node) => node,
            other => bail!("expected map operator, found {other:?}"),
        };
        match &map_node.expressions[0] {
            Expr::Add(lhs, rhs) => {
                assert!(matches!(
                    **lhs,
                    Expr::Literal(ScalarValue::Float64(Some(_)))
                ));
                match rhs.as_ref() {
                    Expr::Literal(ScalarValue::Float64(Some(value))) => {
                        assert_eq!(*value, 2.0);
                    }
                    other => panic!("expected coerced float literal, found {other:?}"),
                }
            }
            other => panic!("expected addition expression, found {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn plans_inner_join_graph() -> Result<()> {
        let right_plan = table_scan(Some("person"), &person_schema(), None)?.build()?;
        let logical_plan = table_scan(Some("auction"), &auction_schema(), None)?
            .join(
                right_plan,
                JoinType::Inner,
                (
                    vec![Column::from_name("seller")],
                    vec![Column::from_name("id")],
                ),
                None,
            )?
            .build()?;

        let planner = QueryPlanner::new();
        let dataflow = planner.plan(&logical_plan, "mv_join_test")?;

        assert_eq!(dataflow.operators.len(), 4);
        let join_node = match &dataflow.operators[2] {
            OperatorNode::Join(node) => node,
            other => panic!("expected join operator, found {other:?}"),
        };
        assert_eq!(join_node.left.operator, OperatorId(0));
        assert_eq!(join_node.right.operator, OperatorId(1));
        assert_eq!(join_node.on, vec![(1, 0)]);

        Ok(())
    }

    #[test]
    fn rejects_non_inner_join() -> Result<()> {
        let right_plan = table_scan(Some("person"), &person_schema(), None)?.build()?;
        let logical_plan = table_scan(Some("auction"), &auction_schema(), None)?
            .join(
                right_plan,
                JoinType::Left,
                (
                    vec![Column::from_name("seller")],
                    vec![Column::from_name("id")],
                ),
                None,
            )?
            .build()?;

        let planner = QueryPlanner::new();
        let err = planner.plan(&logical_plan, "mv_join_test").unwrap_err();
        assert!(err.to_string().contains("only inner joins"));
        Ok(())
    }
}
