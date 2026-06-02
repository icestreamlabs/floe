use super::*;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use dbsp_storage::storage::{KeyValueTable, SlateTable};
use floe_cdc::CdcTableStore;
use floe_cdc_core::{
    CdcChange, CdcRow, CdcRowKey, CdcSourceId, CdcSourcePosition, CdcTableId, CdcTableSchema,
    UpstreamTableRef,
};
use floe_core::RowValue;
use object_store::memory::InMemory;
use slatedb::Db;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use crate::pgoutput_test_messages::{
    TEST_PG_INT8_OID as PG_INT8_OID, TEST_PG_TEXT_OID as PG_TEXT_OID,
    id_status_relation_message as relation_message, insert_id_status_message as insert_message,
    insert_text_message as insert_message_with_values, put_null_value, put_text_value, put_u8,
    put_u16, put_u32, relation_message_with_column_specs as relation_message_with_columns,
    relation_message_with_identity_and_column_specs, truncate_message,
};
use crate::transaction::schema_evolution::POSTGRES_SCHEMA_HISTORY_LIMIT;
use crate::{
    PgOutputMessage, PostgresCdcConfig, PostgresLsn, PostgresReplicationEvent,
    decode_pgoutput_message,
};

const RELATION_ID: u32 = 42;
const OTHER_RELATION_ID: u32 = 43;

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

fn text_tuple(values: impl IntoIterator<Item = Option<String>>) -> Vec<u8> {
    let values = values.into_iter().collect::<Vec<_>>();
    let mut out = Vec::new();
    put_u16(&mut out, values.len() as u16);
    for value in values {
        match value {
            Some(value) => put_text_value(&mut out, &value),
            None => put_null_value(&mut out),
        }
    }
    out
}

fn update_key_message(relation_id: u32, old_id: i64, new_id: i64, status: &str) -> Bytes {
    let mut out = Vec::new();
    put_u8(&mut out, b'U');
    put_u32(&mut out, relation_id);
    put_u8(&mut out, b'K');
    out.extend_from_slice(&text_tuple([Some(old_id.to_string()), None]));
    put_u8(&mut out, b'N');
    out.extend_from_slice(&text_tuple([
        Some(new_id.to_string()),
        Some(status.to_string()),
    ]));
    Bytes::from(out)
}

fn id_status_key(id: i64) -> CdcRowKey {
    CdcRowKey::new([RowValue::Int64(id)]).expect("row key")
}

fn id_status_row(id: i64, status: &str) -> CdcRow {
    CdcRow::new([
        Some(RowValue::Int64(id)),
        Some(RowValue::Utf8(status.to_string())),
    ])
    .expect("row")
}

fn router() -> PostgresTableRouter {
    let mut router = PostgresTableRouter::new();
    router.insert(
        UpstreamTableRef::new("public", "orders").expect("upstream"),
        CdcTableId::new("orders").expect("table id"),
    );
    router
}

fn orders_schema() -> CdcTableSchema {
    schema_for(RELATION_ID, "orders", "orders")
}

fn schema_for(relation_id: u32, upstream_table: &str, table_id: &str) -> CdcTableSchema {
    let PgOutputMessage::Relation(relation) =
        decode_pgoutput_message(relation_message(relation_id, upstream_table))
            .expect("decode relation")
    else {
        panic!("expected relation");
    };
    relation
        .to_cdc_schema(CdcTableId::new(table_id).expect("table id"))
        .expect("schema")
}

fn orders_schemas() -> HashMap<CdcTableId, CdcTableSchema> {
    let schema = orders_schema();
    HashMap::from([(schema.table_id().clone(), schema)])
}

async fn test_store(name: &str) -> CdcTableStore {
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(Db::open(name, object_store).await.expect("open SlateDB"));
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
    CdcTableStore::new(table)
}

enum FakeStep {
    Event(PostgresReplicationEvent),
    End,
    Error(&'static str),
}

struct FakeStream {
    steps: VecDeque<FakeStep>,
    feedbacks: Arc<Mutex<Vec<PostgresLsn>>>,
}

impl FakeStream {
    fn new(
        steps: impl IntoIterator<Item = FakeStep>,
        feedbacks: Arc<Mutex<Vec<PostgresLsn>>>,
    ) -> Self {
        Self {
            steps: steps.into_iter().collect(),
            feedbacks,
        }
    }
}

#[async_trait]
impl PostgresReplicationStream for FakeStream {
    async fn recv_event(&mut self) -> Result<Option<PostgresReplicationEvent>> {
        match self.steps.pop_front().unwrap_or(FakeStep::End) {
            FakeStep::Event(event) => Ok(Some(event)),
            FakeStep::End => Ok(None),
            FakeStep::Error(message) => Err(anyhow!(message)),
        }
    }

    fn update_applied_lsn(&mut self, lsn: PostgresLsn) {
        self.feedbacks.lock().expect("feedback lock").push(lsn);
    }
}

#[derive(Clone)]
struct FakeFactory {
    streams: Arc<Mutex<VecDeque<FakeStream>>>,
    configs: Arc<Mutex<Vec<PostgresCdcConfig>>>,
}

impl FakeFactory {
    fn new(streams: impl IntoIterator<Item = FakeStream>) -> Self {
        Self {
            streams: Arc::new(Mutex::new(streams.into_iter().collect())),
            configs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn configs(&self) -> Vec<PostgresCdcConfig> {
        self.configs.lock().expect("configs lock").clone()
    }
}

#[async_trait]
impl PostgresReplicationClientFactory for FakeFactory {
    type Stream = FakeStream;

    async fn connect(&self, config: &PostgresCdcConfig) -> Result<Self::Stream> {
        self.configs
            .lock()
            .expect("configs lock")
            .push(config.clone());
        self.streams
            .lock()
            .expect("streams lock")
            .pop_front()
            .ok_or_else(|| anyhow!("no fake stream configured"))
    }
}

#[path = "tests/applier.rs"]
mod applier;
#[path = "tests/reconnect.rs"]
mod reconnect;
#[path = "tests/schema_policy.rs"]
mod schema_policy;
#[path = "tests/transaction_grouping.rs"]
mod transaction_grouping;
