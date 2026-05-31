mod asof_extension;
mod circuit;
mod config;
mod context;
mod error;
mod expr;
mod logical_optimizer;

pub use asof_extension::{FloeAsofJoinNode, create_logical_plan_with_asof_preplanner};
pub use circuit::{CircuitNode, CircuitPlan};
pub use config::PlannerConfig;
pub use context::CircuitPlanner;
pub use error::PlannerError;
pub use logical_optimizer::{
    OptimizerDiagnostics, OptimizerRuleDiagnostics, OptimizerStageDiagnostics,
};

#[cfg(test)]
mod tests;
