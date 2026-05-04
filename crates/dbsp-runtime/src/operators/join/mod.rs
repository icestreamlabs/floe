mod op;

pub(crate) use op::JoinTransientInputs;
pub use op::{JoinInputRetention, JoinOp};

#[cfg(test)]
mod tests;
