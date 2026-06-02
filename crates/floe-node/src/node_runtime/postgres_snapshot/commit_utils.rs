use super::*;

pub(super) fn snapshot_checkpoint(
    source_id: &CdcSourceId,
    lsn: PostgresLsn,
) -> Result<CdcCheckpoint> {
    Ok(CdcCheckpoint::new(
        source_id.clone(),
        lsn.to_source_position()?,
        Some(snapshot_transaction_id(lsn)?),
    ))
}

pub(super) fn snapshot_transaction_id(lsn: PostgresLsn) -> Result<CdcTransactionId> {
    CdcTransactionId::new(format!("snapshot:{}", lsn.to_pg_string()))
}

pub(super) async fn wait_for_postgres_snapshot_commit(
    receiver: Option<&mut watch::Receiver<PostgresCdcCommit>>,
    slot: &str,
    target_lsn: PostgresLsn,
    cancel: &CancellationToken,
) -> Result<()> {
    wait_for_postgres_cdc_commit(
        receiver,
        slot,
        target_lsn,
        cancel,
        "initial Postgres snapshot durability",
    )
    .await
}

pub(in crate::node_runtime) async fn wait_for_postgres_cdc_commit(
    receiver: Option<&mut watch::Receiver<PostgresCdcCommit>>,
    slot: &str,
    target_lsn: PostgresLsn,
    cancel: &CancellationToken,
    operation: &str,
) -> Result<()> {
    let Some(receiver) = receiver else {
        bail!("cannot wait for {operation} without Postgres CDC commit receiver");
    };

    loop {
        let commit = receiver.borrow_and_update().clone();
        if postgres_commit_covers_lsn(&commit, slot, target_lsn)? {
            return Ok(());
        }

        tokio::select! {
            _ = cancel.cancelled() => {
                bail!("cancelled while waiting for {operation}");
            }
            changed = receiver.changed() => {
                changed.with_context(|| {
                    format!("Postgres CDC commit channel closed before {operation} completed")
                })?;
            }
        }
    }
}

pub(super) fn postgres_commit_covers_lsn(
    commit: &PostgresCdcCommit,
    slot: &str,
    target_lsn: PostgresLsn,
) -> Result<bool> {
    let Some(slot_commit) = commit.slots.iter().find(|entry| entry.slot == slot) else {
        return Ok(false);
    };
    Ok(PostgresLsn::parse(&slot_commit.lsn)?.as_u64() >= target_lsn.as_u64())
}

pub(super) fn qualified_table_name(upstream: &UpstreamTableRef) -> String {
    format!(
        "{}.{}",
        quote_pg_ident(upstream.schema()),
        quote_pg_ident(upstream.table())
    )
}

pub(super) fn quote_pg_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(super) fn quote_pg_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
