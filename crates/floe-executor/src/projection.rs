use std::sync::Arc;

use anyhow::Result;
use dbsp::circuit::RowSchema;
use dbsp::circuit::plan::DbspProjectExpr;

use crate::expression::ExpressionEvaluator;
use crate::stream_types::Row;

pub struct ProjectionEvaluator {
    evaluators: Vec<ExpressionEvaluator>,
}

impl ProjectionEvaluator {
    pub fn new(input_schema: Arc<RowSchema>, exprs: &[DbspProjectExpr]) -> Self {
        let evaluators = exprs
            .iter()
            .map(|expr| ExpressionEvaluator::new(Arc::clone(&input_schema), expr.expression()))
            .collect();
        Self { evaluators }
    }

    pub fn project(&self, input: &Row) -> Result<Row> {
        let mut output = Vec::with_capacity(self.evaluators.len());
        for evaluator in &self.evaluators {
            output.push(evaluator.eval(input)?);
        }
        Ok(output)
    }
}
