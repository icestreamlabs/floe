mod op;

#[cfg(test)]
pub(crate) use op::JoinInputRetention;
#[cfg(test)]
pub(crate) use op::JoinTransientInputs;
pub use op::{JoinBatchConfig, JoinClosedIndexConfig, JoinOp};

#[cfg(test)]
mod tests;
