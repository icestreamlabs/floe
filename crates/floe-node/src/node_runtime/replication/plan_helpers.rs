use std::collections::HashSet;

use floe_cdc_core::{CdcCheckpoint, CdcSourceId, CdcTableId, ChangeBatch, TransactionBatch};

use super::super::{ReplicationPipelineRuntimePlan, ReplicationPipelineRuntimeTarget};

pub(super) fn ordered_replication_plans_for_transaction<'a>(
    plans: &'a [ReplicationPipelineRuntimePlan],
    transaction: &TransactionBatch,
) -> Vec<&'a ReplicationPipelineRuntimePlan> {
    let mut ordered = plans.iter().collect::<Vec<_>>();
    if ordered.len() <= 1 || !replication_pipeline_targets_are_distinct(plans) {
        return ordered;
    }
    ordered.sort_by(|left, right| {
        transaction_change_count_for_table(transaction, &right.table_id).cmp(
            &transaction_change_count_for_table(transaction, &left.table_id),
        )
    });
    ordered
}

pub(super) fn replication_pipeline_targets_are_distinct(
    plans: &[ReplicationPipelineRuntimePlan],
) -> bool {
    let mut targets = HashSet::with_capacity(plans.len());
    plans
        .iter()
        .all(|plan| targets.insert(replication_pipeline_target_identity(plan)))
}

fn replication_pipeline_target_identity(plan: &ReplicationPipelineRuntimePlan) -> String {
    match &plan.target {
        ReplicationPipelineRuntimeTarget::Kafka { brokers, topic } => {
            format!("kafka\0{brokers}\0{topic}")
        }
        ReplicationPipelineRuntimeTarget::Postgres { connection, table } => {
            format!("postgres\0{connection}\0{table}")
        }
    }
}

fn transaction_change_count_for_table(
    transaction: &TransactionBatch,
    table_id: &CdcTableId,
) -> usize {
    transaction
        .change_batches()
        .iter()
        .filter(|batch| batch.table_id() == table_id)
        .map(ChangeBatch::change_count)
        .sum()
}

pub(in crate::node_runtime) fn replication_pipeline_table_id(
    source_name: &str,
    upstream_table: &str,
) -> anyhow::Result<CdcTableId> {
    CdcTableId::new(format!("{source_name}:{upstream_table}"))
}

pub(in crate::node_runtime) fn materialized_transaction(
    source_id: &CdcSourceId,
    materialized_table_ids: &HashSet<CdcTableId>,
    transaction: &TransactionBatch,
) -> anyhow::Result<Option<TransactionBatch>> {
    let change_batches = transaction
        .change_batches()
        .iter()
        .filter(|batch| materialized_table_ids.contains(batch.table_id()))
        .cloned()
        .collect::<Vec<_>>();
    if change_batches.is_empty() {
        return Ok(None);
    }
    Ok(Some(TransactionBatch::new(
        source_id.clone(),
        transaction.transaction_id().cloned(),
        transaction.start_position().cloned(),
        transaction.commit_position().clone(),
        change_batches,
    )?))
}

pub(in crate::node_runtime) fn pipeline_checkpoint_from_transaction(
    transaction: &TransactionBatch,
) -> CdcCheckpoint {
    CdcCheckpoint::new(
        transaction.source_id().clone(),
        transaction.commit_position().clone(),
        transaction.transaction_id().cloned(),
    )
    .with_schema_versions(transaction.schema_versions().clone())
}
