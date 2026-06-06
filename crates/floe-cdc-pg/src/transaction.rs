mod applier;
mod assembler;
mod router;
mod schema_evolution;

pub use applier::{
    PostgresCdcApplyOutcome, PostgresCdcEventApplier, PostgresCdcLagSnapshot, PostgresCdcTableLag,
};
pub use assembler::PostgresTransactionAssembler;
pub use router::PostgresTableRouter;
pub use schema_evolution::{
    PostgresSchemaEvolutionObservation, PostgresSchemaEvolutionOutcome,
    PostgresSchemaEvolutionPolicy,
};

#[cfg(test)]
mod tests;
