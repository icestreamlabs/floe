use std::collections::HashMap;

use anyhow::{Result, anyhow, bail};
use floe_cdc_core::{
    CdcChange, CdcSourceId, CdcSourcePosition, CdcTableId, CdcTransactionId, ChangeBatch,
    TransactionBatch, UpstreamTableRef,
};

use crate::{PgOutputCdcChange, PgOutputDecoder, PostgresLsn, PostgresReplicationEvent};

#[derive(Debug, Clone, Default)]
pub struct PostgresTableRouter {
    by_upstream_table: HashMap<UpstreamTableRef, CdcTableId>,
}

impl PostgresTableRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, upstream_table: UpstreamTableRef, table_id: CdcTableId) {
        self.by_upstream_table.insert(upstream_table, table_id);
    }

    pub fn get(&self, upstream_table: &UpstreamTableRef) -> Option<&CdcTableId> {
        self.by_upstream_table.get(upstream_table)
    }
}

pub struct PostgresTransactionAssembler {
    source_id: CdcSourceId,
    router: PostgresTableRouter,
    decoder: PgOutputDecoder,
    current: Option<InFlightTransaction>,
}

impl PostgresTransactionAssembler {
    pub fn new(source_id: CdcSourceId, router: PostgresTableRouter) -> Self {
        Self {
            source_id,
            router,
            decoder: PgOutputDecoder::new(),
            current: None,
        }
    }

    pub fn accept_event(
        &mut self,
        event: PostgresReplicationEvent,
    ) -> Result<Option<TransactionBatch>> {
        match event {
            PostgresReplicationEvent::Begin { xid, .. } => {
                self.begin(xid)?;
                Ok(None)
            }
            PostgresReplicationEvent::XLogData { data, .. } => {
                self.accept_xlog_data(data)?;
                Ok(None)
            }
            PostgresReplicationEvent::Commit { end_lsn, .. } => self.commit(end_lsn),
            PostgresReplicationEvent::KeepAlive { .. }
            | PostgresReplicationEvent::Message { .. }
            | PostgresReplicationEvent::StoppedAt { .. } => Ok(None),
        }
    }

    pub fn decoder(&self) -> &PgOutputDecoder {
        &self.decoder
    }

    fn begin(&mut self, xid: u32) -> Result<()> {
        if self.current.is_some() {
            bail!("Postgres CDC transaction began before previous transaction committed");
        }
        self.current = Some(InFlightTransaction::new(xid)?);
        Ok(())
    }

    fn accept_xlog_data(&mut self, data: bytes::Bytes) -> Result<()> {
        let Some(change) = self.decoder.decode_cdc_change(data)? else {
            return Ok(());
        };
        let table_id = self.route_change(&change)?;
        let current = self
            .current
            .as_mut()
            .ok_or_else(|| anyhow!("Postgres CDC change arrived outside a transaction boundary"))?;
        current.push(table_id, change.into_change());
        Ok(())
    }

    fn route_change(&self, change: &PgOutputCdcChange) -> Result<Option<CdcTableId>> {
        let upstream_table = change.relation().upstream_table_ref()?;
        Ok(self.router.get(&upstream_table).cloned())
    }

    fn commit(&mut self, end_lsn: PostgresLsn) -> Result<Option<TransactionBatch>> {
        let current = self
            .current
            .take()
            .ok_or_else(|| anyhow!("Postgres CDC commit arrived without a begin boundary"))?;
        if current.table_changes.is_empty() {
            return Ok(None);
        }
        let change_batches = current
            .table_changes
            .into_iter()
            .map(|table_changes| ChangeBatch::new(table_changes.table_id, table_changes.changes))
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(TransactionBatch::new(
            self.source_id.clone(),
            Some(current.transaction_id),
            None,
            CdcSourcePosition::postgres(end_lsn.to_pg_string(), None)?,
            change_batches,
        )?))
    }
}

struct InFlightTransaction {
    transaction_id: CdcTransactionId,
    table_changes: Vec<TableChanges>,
}

impl InFlightTransaction {
    fn new(xid: u32) -> Result<Self> {
        Ok(Self {
            transaction_id: CdcTransactionId::new(format!("pg-xid-{xid}"))?,
            table_changes: Vec::new(),
        })
    }

    fn push(&mut self, table_id: Option<CdcTableId>, change: CdcChange) {
        let Some(table_id) = table_id else {
            return;
        };
        if let Some(existing) = self
            .table_changes
            .iter_mut()
            .find(|existing| existing.table_id == table_id)
        {
            existing.changes.push(change);
        } else {
            self.table_changes.push(TableChanges {
                table_id,
                changes: vec![change],
            });
        }
    }
}

struct TableChanges {
    table_id: CdcTableId,
    changes: Vec<CdcChange>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use floe_cdc_core::{CdcRow, UpstreamTableRef};
    use floe_core::RowValue;

    const RELATION_ID: u32 = 42;
    const OTHER_RELATION_ID: u32 = 43;
    const PG_INT8_OID: u32 = 20;
    const PG_TEXT_OID: u32 = 25;

    fn put_u8(out: &mut Vec<u8>, value: u8) {
        out.push(value);
    }

    fn put_u16(out: &mut Vec<u8>, value: u16) {
        out.extend_from_slice(&value.to_be_bytes());
    }

    fn put_u32(out: &mut Vec<u8>, value: u32) {
        out.extend_from_slice(&value.to_be_bytes());
    }

    fn put_i32(out: &mut Vec<u8>, value: i32) {
        out.extend_from_slice(&value.to_be_bytes());
    }

    fn put_cstring(out: &mut Vec<u8>, value: &str) {
        out.extend_from_slice(value.as_bytes());
        out.push(0);
    }

    fn put_text_value(out: &mut Vec<u8>, value: &str) {
        put_u8(out, b't');
        put_i32(out, value.len() as i32);
        out.extend_from_slice(value.as_bytes());
    }

    fn relation_message(relation_id: u32, table: &str) -> Bytes {
        let mut out = Vec::new();
        put_u8(&mut out, b'R');
        put_u32(&mut out, relation_id);
        put_cstring(&mut out, "public");
        put_cstring(&mut out, table);
        put_u8(&mut out, b'd');
        put_u16(&mut out, 2);

        put_u8(&mut out, 1);
        put_cstring(&mut out, "id");
        put_u32(&mut out, PG_INT8_OID);
        put_i32(&mut out, -1);

        put_u8(&mut out, 0);
        put_cstring(&mut out, "status");
        put_u32(&mut out, PG_TEXT_OID);
        put_i32(&mut out, -1);

        Bytes::from(out)
    }

    fn insert_message(relation_id: u32, id: i64, status: &str) -> Bytes {
        let mut out = Vec::new();
        put_u8(&mut out, b'I');
        put_u32(&mut out, relation_id);
        put_u8(&mut out, b'N');
        put_u16(&mut out, 2);
        put_text_value(&mut out, &id.to_string());
        put_text_value(&mut out, status);
        Bytes::from(out)
    }

    fn begin(xid: u32) -> PostgresReplicationEvent {
        PostgresReplicationEvent::Begin {
            final_lsn: PostgresLsn::from_u64(10),
            xid,
            commit_time_micros: 100,
        }
    }

    fn xlog(data: Bytes) -> PostgresReplicationEvent {
        PostgresReplicationEvent::XLogData {
            wal_start: PostgresLsn::from_u64(11),
            wal_end: PostgresLsn::from_u64(12),
            server_time_micros: 101,
            data,
        }
    }

    fn commit(end_lsn: u64) -> PostgresReplicationEvent {
        PostgresReplicationEvent::Commit {
            lsn: PostgresLsn::from_u64(end_lsn - 1),
            end_lsn: PostgresLsn::from_u64(end_lsn),
            commit_time_micros: 102,
        }
    }

    fn router() -> PostgresTableRouter {
        let mut router = PostgresTableRouter::new();
        router.insert(
            UpstreamTableRef::new("public", "orders").expect("upstream"),
            CdcTableId::new("orders").expect("table id"),
        );
        router
    }

    #[test]
    fn assembles_decoded_changes_into_transaction_batch() {
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let mut assembler = PostgresTransactionAssembler::new(source_id.clone(), router());

        assert!(
            assembler
                .accept_event(xlog(relation_message(RELATION_ID, "orders")))
                .expect("relation metadata")
                .is_none()
        );
        assembler.accept_event(begin(55)).expect("begin");
        assembler
            .accept_event(xlog(insert_message(RELATION_ID, 7, "open")))
            .expect("insert");
        let transaction = assembler
            .accept_event(commit(30))
            .expect("commit")
            .expect("transaction");

        assert_eq!(transaction.source_id(), &source_id);
        assert_eq!(
            transaction.transaction_id().expect("txid").as_str(),
            "pg-xid-55"
        );
        assert_eq!(
            transaction.commit_position(),
            &CdcSourcePosition::Postgres {
                commit_lsn: "0/1E".to_string(),
                event_lsn: None
            }
        );
        assert_eq!(transaction.change_batches().len(), 1);
        assert_eq!(
            transaction.change_batches()[0].table_id(),
            &CdcTableId::new("orders").expect("table id")
        );
        assert_eq!(
            transaction.change_batches()[0].changes(),
            &[CdcChange::Insert {
                row: CdcRow::new([
                    Some(RowValue::Int64(7)),
                    Some(RowValue::Utf8("open".to_string())),
                ])
                .expect("row")
            }]
        );
    }

    #[test]
    fn groups_multiple_tables_in_one_source_transaction() {
        let mut router = router();
        router.insert(
            UpstreamTableRef::new("public", "customers").expect("upstream"),
            CdcTableId::new("customers").expect("table id"),
        );
        let mut assembler =
            PostgresTransactionAssembler::new(CdcSourceId::new("pg_main").expect("source"), router);

        assembler
            .accept_event(xlog(relation_message(RELATION_ID, "orders")))
            .expect("orders relation");
        assembler
            .accept_event(xlog(relation_message(OTHER_RELATION_ID, "customers")))
            .expect("customers relation");
        assembler.accept_event(begin(56)).expect("begin");
        assembler
            .accept_event(xlog(insert_message(RELATION_ID, 7, "open")))
            .expect("orders insert");
        assembler
            .accept_event(xlog(insert_message(OTHER_RELATION_ID, 8, "active")))
            .expect("customers insert");

        let transaction = assembler
            .accept_event(commit(40))
            .expect("commit")
            .expect("transaction");
        let tables: Vec<&str> = transaction
            .change_batches()
            .iter()
            .map(|batch| batch.table_id().as_str())
            .collect();
        assert_eq!(tables, vec!["orders", "customers"]);
    }

    #[test]
    fn ignores_unrouted_tables_and_empty_transactions() {
        let mut assembler = PostgresTransactionAssembler::new(
            CdcSourceId::new("pg_main").expect("source"),
            router(),
        );
        assembler
            .accept_event(xlog(relation_message(OTHER_RELATION_ID, "unmapped")))
            .expect("relation");
        assembler.accept_event(begin(57)).expect("begin");
        assembler
            .accept_event(xlog(insert_message(OTHER_RELATION_ID, 7, "ignored")))
            .expect("ignored insert");
        assert!(
            assembler
                .accept_event(commit(50))
                .expect("commit")
                .is_none()
        );
    }

    #[test]
    fn rejects_dml_outside_transaction_boundary() {
        let mut assembler = PostgresTransactionAssembler::new(
            CdcSourceId::new("pg_main").expect("source"),
            router(),
        );
        assembler
            .accept_event(xlog(relation_message(RELATION_ID, "orders")))
            .expect("relation");
        let err = assembler
            .accept_event(xlog(insert_message(RELATION_ID, 7, "open")))
            .expect_err("dml outside transaction should fail");
        assert!(format!("{err:#}").contains("outside a transaction"));
    }
}
