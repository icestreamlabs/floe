use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::logical_expr::{
    Expr, Extension, JoinType, LogicalPlan, UserDefinedLogicalNodeCore,
};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchemaRef, Result as DataFusionResult, ScalarValue, plan_err};
use datafusion_sql::parser::Statement as DFStatement;
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, JoinConstraint, JoinOperator,
    Query as SqlQuery, SetExpr, Statement as SqlStatement, TableFactor, TableWithJoins, Value,
};

const ASOF_MARKER_PREFIX: &str = "__floe_asof_marker_";

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FloeAsofJoinNode {
    left: Arc<LogicalPlan>,
    right: Arc<LogicalPlan>,
    join_type: JoinType,
    on: Vec<(Expr, Expr)>,
    filter: Option<Expr>,
    schema: DFSchemaRef,
}

impl PartialOrd for FloeAsofJoinNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(format!("{self:?}").cmp(&format!("{other:?}")))
    }
}

impl FloeAsofJoinNode {
    pub fn try_new(
        left: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        join_type: JoinType,
        on: Vec<(Expr, Expr)>,
        filter: Option<Expr>,
        schema: DFSchemaRef,
    ) -> DataFusionResult<Self> {
        if !matches!(join_type, JoinType::Inner | JoinType::Left) {
            return plan_err!("Floe ASOF logical node supports INNER and LEFT joins only");
        }
        Ok(Self {
            left,
            right,
            join_type,
            on,
            filter,
            schema,
        })
    }

    pub fn left(&self) -> &LogicalPlan {
        self.left.as_ref()
    }

    pub fn right(&self) -> &LogicalPlan {
        self.right.as_ref()
    }

    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub fn on(&self) -> &[(Expr, Expr)] {
        &self.on
    }

    pub fn filter(&self) -> Option<&Expr> {
        self.filter.as_ref()
    }
}

impl UserDefinedLogicalNodeCore for FloeAsofJoinNode {
    fn name(&self) -> &str {
        "FloeAsofJoin"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.left.as_ref(), self.right.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut exprs = Vec::with_capacity(self.on.len() * 2 + usize::from(self.filter.is_some()));
        for (left, right) in &self.on {
            exprs.push(left.clone());
            exprs.push(right.clone());
        }
        if let Some(filter) = &self.filter {
            exprs.push(filter.clone());
        }
        exprs
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "FloeAsofJoin: type={:?}, keys={}, filter={}",
            self.join_type,
            self.on.len(),
            self.filter
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "<none>".to_string())
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if inputs.len() != 2 {
            return plan_err!("Floe ASOF logical node requires exactly two inputs");
        }
        let expected = self.on.len() * 2 + usize::from(self.filter.is_some());
        if exprs.len() != expected {
            return plan_err!(
                "Floe ASOF logical node expected {expected} expressions, got {}",
                exprs.len()
            );
        }

        let mut iter = exprs.into_iter();
        let mut on = Vec::with_capacity(self.on.len());
        for _ in 0..self.on.len() {
            let Some(left) = iter.next() else {
                return plan_err!("Floe ASOF logical node missing left ON expression");
            };
            let Some(right) = iter.next() else {
                return plan_err!("Floe ASOF logical node missing right ON expression");
            };
            on.push((left, right));
        }
        let filter = if self.filter.is_some() {
            let Some(filter) = iter.next() else {
                return plan_err!("Floe ASOF logical node missing filter expression");
            };
            Some(filter)
        } else {
            None
        };

        Ok(Self {
            left: Arc::new(inputs[0].clone()),
            right: Arc::new(inputs[1].clone()),
            join_type: self.join_type,
            on,
            filter,
            schema: self.schema.clone(),
        })
    }
}

pub async fn create_logical_plan_with_asof_preplanner(
    state: &SessionState,
    sql: &str,
) -> DataFusionResult<LogicalPlan> {
    let dialect = state.config_options().sql_parser.dialect;
    let mut statement = state.sql_to_statement(sql, &dialect)?;
    let asof_count = rewrite_asof_joins(&mut statement)?;
    if asof_count == 0 {
        return state.statement_to_plan(statement).await;
    }

    let plan = state.statement_to_plan(statement).await?;
    install_asof_extension_nodes(plan, asof_count)
}

fn install_asof_extension_nodes(
    plan: LogicalPlan,
    expected_count: usize,
) -> DataFusionResult<LogicalPlan> {
    let mut converted_count = 0usize;
    let transformed = plan.transform_up(|plan| match plan {
        LogicalPlan::Join(mut join) => {
            let Some(filter) = join.filter.take() else {
                return Ok(Transformed::no(LogicalPlan::Join(join)));
            };
            let (filter, had_marker) = strip_asof_marker(filter);
            if !had_marker {
                join.filter = filter;
                return Ok(Transformed::no(LogicalPlan::Join(join)));
            }
            converted_count += 1;
            let node = FloeAsofJoinNode::try_new(
                join.left,
                join.right,
                join.join_type,
                join.on,
                filter,
                join.schema,
            )?;
            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(node),
            })))
        }
        other => Ok(Transformed::no(other)),
    })?;

    if converted_count != expected_count {
        return plan_err!(
            "expected to convert {expected_count} ASOF join(s), converted {converted_count}"
        );
    }
    Ok(transformed.data)
}

fn rewrite_asof_joins(statement: &mut DFStatement) -> DataFusionResult<usize> {
    match statement {
        DFStatement::Statement(statement) => rewrite_sql_statement(statement),
        _ => Ok(0),
    }
}

fn rewrite_sql_statement(statement: &mut SqlStatement) -> DataFusionResult<usize> {
    match statement {
        SqlStatement::Query(query) => rewrite_query(query),
        _ => Ok(0),
    }
}

fn rewrite_query(query: &mut Box<SqlQuery>) -> DataFusionResult<usize> {
    let mut count = 0usize;
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            count += rewrite_query(&mut cte.query)?;
        }
    }
    count += rewrite_set_expr(&mut query.body)?;
    Ok(count)
}

fn rewrite_set_expr(set_expr: &mut Box<SetExpr>) -> DataFusionResult<usize> {
    match set_expr.as_mut() {
        SetExpr::Select(select) => {
            let mut count = 0usize;
            for table in &mut select.from {
                count += rewrite_table_with_joins(table)?;
            }
            Ok(count)
        }
        SetExpr::Query(query) => rewrite_query(query),
        SetExpr::SetOperation { left, right, .. } => {
            Ok(rewrite_set_expr(left)? + rewrite_set_expr(right)?)
        }
        _ => Ok(0),
    }
}

fn rewrite_table_with_joins(table: &mut TableWithJoins) -> DataFusionResult<usize> {
    let mut count = rewrite_table_factor(&mut table.relation)?;
    for join in &mut table.joins {
        count += rewrite_table_factor(&mut join.relation)?;
        let JoinOperator::AsOf {
            match_condition,
            constraint,
        } = &join.join_operator
        else {
            continue;
        };

        let marker = marker_expr(count);
        let combined = match constraint.clone() {
            JoinConstraint::On(on) => and_expr(and_expr(on, match_condition.clone()), marker),
            JoinConstraint::None => and_expr(match_condition.clone(), marker),
            other => {
                return plan_err!(
                    "ASOF JOIN currently supports ON or no join constraint, found {other:?}"
                );
            }
        };
        join.join_operator = JoinOperator::LeftOuter(JoinConstraint::On(combined));
        count += 1;
    }
    Ok(count)
}

fn rewrite_table_factor(table_factor: &mut TableFactor) -> DataFusionResult<usize> {
    match table_factor {
        TableFactor::Derived { subquery, .. } => rewrite_query(subquery),
        _ => Ok(0),
    }
}

fn and_expr(left: SqlExpr, right: SqlExpr) -> SqlExpr {
    SqlExpr::BinaryOp {
        left: Box::new(left),
        op: SqlBinaryOperator::And,
        right: Box::new(right),
    }
}

fn marker_expr(index: usize) -> SqlExpr {
    let marker = format!("{ASOF_MARKER_PREFIX}{index}");
    let literal = || SqlExpr::Value(Value::SingleQuotedString(marker.clone()).into());
    SqlExpr::BinaryOp {
        left: Box::new(literal()),
        op: SqlBinaryOperator::Eq,
        right: Box::new(literal()),
    }
}

fn strip_asof_marker(expr: Expr) -> (Option<Expr>, bool) {
    let mut conjuncts = Vec::new();
    flatten_conjuncts(expr, &mut conjuncts);
    let mut had_marker = false;
    let residuals = conjuncts
        .into_iter()
        .filter(|expr| {
            if is_asof_marker(expr) {
                had_marker = true;
                false
            } else {
                true
            }
        })
        .collect::<Vec<_>>();
    (combine_conjuncts(residuals), had_marker)
}

fn flatten_conjuncts(expr: Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == datafusion::logical_expr::Operator::And => {
            flatten_conjuncts(*binary.left, out);
            flatten_conjuncts(*binary.right, out);
        }
        other => out.push(other),
    }
}

fn combine_conjuncts(conjuncts: Vec<Expr>) -> Option<Expr> {
    let mut iter = conjuncts.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |left, right| {
        Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
            left: Box::new(left),
            op: datafusion::logical_expr::Operator::And,
            right: Box::new(right),
        })
    }))
}

fn is_asof_marker(expr: &Expr) -> bool {
    let Expr::BinaryExpr(binary) = expr else {
        return false;
    };
    if binary.op != datafusion::logical_expr::Operator::Eq {
        return false;
    }
    let Some(left) = marker_literal(binary.left.as_ref()) else {
        return false;
    };
    let Some(right) = marker_literal(binary.right.as_ref()) else {
        return false;
    };
    left == right
}

fn marker_literal(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _)
            if value.starts_with(ASOF_MARKER_PREFIX) =>
        {
            Some(value.as_str())
        }
        _ => None,
    }
}
