use std::sync::Arc;

use anyhow::Result;
use dbsp::circuit::plan::DbspJoinNode;

use crate::expression::{ExpressionEvaluator, scalar_equals};
use crate::stream_types::Row;

pub struct JoinEvaluator {
    key_evaluators: Vec<(ExpressionEvaluator, ExpressionEvaluator)>,
    residual: Option<ExpressionEvaluator>,
}

impl JoinEvaluator {
    pub fn new(node: &DbspJoinNode) -> Self {
        let left_schema = Arc::clone(&node.left_schema);
        let right_schema = Arc::clone(&node.right_schema);
        let key_evaluators = node
            .keys
            .iter()
            .map(|key| {
                (
                    ExpressionEvaluator::new(Arc::clone(&left_schema), key.left_expression()),
                    ExpressionEvaluator::new(Arc::clone(&right_schema), key.right_expression()),
                )
            })
            .collect();
        let residual = node
            .residual
            .as_ref()
            .map(|expr| ExpressionEvaluator::new(Arc::clone(&node.output_schema), expr));

        Self {
            key_evaluators,
            residual,
        }
    }

    pub fn matches(&self, left: &Row, right: &Row) -> Result<bool> {
        for (left_eval, right_eval) in &self.key_evaluators {
            let left_value = left_eval.eval(left)?;
            let right_value = right_eval.eval(right)?;
            if !scalar_equals(&left_value, &right_value)?.unwrap_or(false) {
                return Ok(false);
            }
        }

        if let Some(residual) = &self.residual {
            let combined = self.combine(left, right);
            residual.eval_bool(&combined)
        } else {
            Ok(true)
        }
    }

    pub fn left_key(&self, left: &Row) -> Result<Option<Row>> {
        let mut values = Vec::with_capacity(self.key_evaluators.len());
        for (left_eval, _) in &self.key_evaluators {
            let value = left_eval.eval(left)?;
            if value.is_null() {
                return Ok(None);
            }
            values.push(value);
        }
        Ok(Some(values))
    }

    pub fn right_key(&self, right: &Row) -> Result<Option<Row>> {
        let mut values = Vec::with_capacity(self.key_evaluators.len());
        for (_, right_eval) in &self.key_evaluators {
            let value = right_eval.eval(right)?;
            if value.is_null() {
                return Ok(None);
            }
            values.push(value);
        }
        Ok(Some(values))
    }

    pub fn residual_matches(&self, left: &Row, right: &Row) -> Result<bool> {
        if let Some(residual) = &self.residual {
            let combined = self.combine(left, right);
            residual.eval_bool(&combined)
        } else {
            Ok(true)
        }
    }

    pub fn has_residual(&self) -> bool {
        self.residual.is_some()
    }

    pub fn project(&self, left: &Row, right: &Row) -> Row {
        self.combine(left, right)
    }

    fn combine(&self, left: &Row, right: &Row) -> Row {
        let mut combined = Vec::with_capacity(left.len() + right.len());
        combined.extend_from_slice(left);
        combined.extend_from_slice(right);
        combined
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Column;
    use datafusion::logical_expr::Expr as DfExpr;
    use datafusion::scalar::ScalarValue;
    use dbsp::circuit::plan::{DbspJoinNode, DbspJoinType};
    use dbsp::circuit::schema::{Field, RowSchema};
    use dbsp::circuit::types::DbspScalarType;
    use std::sync::Arc;

    #[test]
    fn join_key_nulls_do_not_match() {
        let schema = Arc::new(
            RowSchema::try_new(vec![Field::new("id", DbspScalarType::Int64, true)])
                .expect("schema"),
        );
        let left_expr = DfExpr::Column(Column::new_unqualified("id".to_string()));
        let right_expr = DfExpr::Column(Column::new_unqualified("id".to_string()));
        let node = DbspJoinNode::try_new(
            DbspJoinType::Inner,
            Arc::clone(&schema),
            Arc::clone(&schema),
            vec![(left_expr, right_expr)],
            None,
        )
        .expect("join node");
        let evaluator = JoinEvaluator::new(&node);

        let left = vec![ScalarValue::Int64(None)];
        let right = vec![ScalarValue::Int64(Some(1))];
        assert!(!evaluator.matches(&left, &right).expect("join matches"));
    }
}
