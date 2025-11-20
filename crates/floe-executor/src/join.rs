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
            if !scalar_equals(&left_value, &right_value)? {
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
