mod applier;
mod apply_loop;
mod assembler;
mod router;
mod schema_evolution;

pub use applier::{
    PostgresCdcApplyOutcome, PostgresCdcEventApplier, PostgresCdcLagSnapshot, PostgresCdcTableLag,
};
pub use apply_loop::{
    PgWireReplicationClientFactory, PostgresCdcReconnectPolicy, PostgresReplicationClientFactory,
    PostgresReplicationStream, run_postgres_cdc_apply_loop,
    run_postgres_cdc_apply_loop_with_reconnect,
};
pub use assembler::PostgresTransactionAssembler;
pub use router::PostgresTableRouter;
pub use schema_evolution::{
    PostgresSchemaEvolutionObservation, PostgresSchemaEvolutionOutcome,
    PostgresSchemaEvolutionPolicy,
};

#[cfg(test)]
mod tests;
