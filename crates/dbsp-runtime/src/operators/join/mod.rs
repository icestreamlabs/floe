mod op;

#[cfg(test)]
pub(crate) use op::JoinInputRetention;
pub use op::JoinOp;
#[cfg(test)]
pub(crate) use op::JoinTransientInputs;

#[cfg(test)]
mod tests;
