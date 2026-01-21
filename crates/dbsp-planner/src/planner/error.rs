use std::fmt::{self, Display};

use anyhow::Error as AnyError;

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
