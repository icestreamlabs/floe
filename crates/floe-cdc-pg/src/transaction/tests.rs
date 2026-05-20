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
    insert_text_message as insert_message_with_values,
    relation_message_with_column_specs as relation_message_with_columns, truncate_message,
};
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
fn compatible_column_additions_can_be_projected_to_catalog_schema() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let mut assembler = PostgresTransactionAssembler::with_schemas(
        source_id,
        router(),
        orders_schemas(),
        PostgresSchemaEvolutionPolicy::IgnoreCompatible,
    );
    let relation = relation_message_with_columns(
        RELATION_ID,
        "orders",
        &[
            ("id", PG_INT8_OID, true),
            ("status", PG_TEXT_OID, false),
            ("note", PG_TEXT_OID, false),
        ],
    );

    assembler
        .accept_event(xlog(relation.clone()))
        .expect("compatible relation");
    let observations = assembler.drain_schema_evolution_observations();
    assert_eq!(observations.len(), 1);
    let observation = &observations[0];
    assert_eq!(observation.table_id().as_str(), "orders");
    assert_eq!(observation.upstream_table().schema(), "public");
    assert_eq!(observation.upstream_table().table(), "orders");
    assert_eq!(
        observation.policy(),
        PostgresSchemaEvolutionPolicy::IgnoreCompatible
    );
    assert_eq!(
        observation.outcome(),
        PostgresSchemaEvolutionOutcome::CompatibleAddition
    );
    assert_eq!(observation.added_columns(), &["note".to_string()]);
    assert_eq!(observation.reason(), None);
    assembler.accept_event(begin(57)).expect("begin");
    assembler
        .accept_event(xlog(insert_message_with_values(
            RELATION_ID,
            &["7".to_string(), "open".to_string(), "ignored".to_string()],
        )))
        .expect("insert");
    let transaction = assembler
        .accept_event(commit(41))
        .expect("commit")
        .expect("transaction");

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
    let PgOutputMessage::Relation(observed_relation) =
        decode_pgoutput_message(relation).expect("decode relation")
    else {
        panic!("expected relation");
    };
    let observed_schema = observed_relation
        .to_cdc_schema(CdcTableId::new("orders").expect("table id"))
        .expect("observed schema");
    assert_eq!(
        transaction.schema_versions().get("orders").copied(),
        Some(observed_schema.stable_fingerprint())
    );
}

#[test]
fn fail_fast_schema_policy_rejects_compatible_additions() {
    let mut assembler = PostgresTransactionAssembler::with_schemas(
        CdcSourceId::new("pg_main").expect("source id"),
        router(),
        orders_schemas(),
        PostgresSchemaEvolutionPolicy::FailFast,
    );
    let err = assembler
        .accept_event(xlog(relation_message_with_columns(
            RELATION_ID,
            "orders",
            &[
                ("id", PG_INT8_OID, true),
                ("status", PG_TEXT_OID, false),
                ("note", PG_TEXT_OID, false),
            ],
        )))
        .expect_err("compatible addition should fail under fail-fast");

    assert!(format!("{err:#}").contains("compatible column additions"));
    let observations = assembler.drain_schema_evolution_observations();
    assert_eq!(observations.len(), 1);
    let observation = &observations[0];
    assert_eq!(
        observation.policy(),
        PostgresSchemaEvolutionPolicy::FailFast
    );
    assert_eq!(
        observation.outcome(),
        PostgresSchemaEvolutionOutcome::RejectedCompatibleAddition
    );
    assert_eq!(observation.added_columns(), &["note".to_string()]);
    assert_eq!(
        observation.reason(),
        Some("compatible column additions rejected by fail-fast policy")
    );
}

#[test]
fn schema_policy_rejects_incompatible_type_changes() {
    let mut assembler = PostgresTransactionAssembler::with_schemas(
        CdcSourceId::new("pg_main").expect("source id"),
        router(),
        orders_schemas(),
        PostgresSchemaEvolutionPolicy::IgnoreCompatible,
    );
    let err = assembler
        .accept_event(xlog(relation_message_with_columns(
            RELATION_ID,
            "orders",
            &[("id", PG_INT8_OID, true), ("status", PG_INT8_OID, false)],
        )))
        .expect_err("type change should fail");

    assert!(format!("{err:#}").contains("type changed"));
    let observations = assembler.drain_schema_evolution_observations();
    assert_eq!(observations.len(), 1);
    let observation = &observations[0];
    assert_eq!(
        observation.outcome(),
        PostgresSchemaEvolutionOutcome::Incompatible
    );
    assert_eq!(observation.added_columns(), &[] as &[String]);
    assert!(
        observation
            .reason()
            .expect("reason")
            .contains("type changed")
    );
}

#[test]
fn schema_policy_rejects_dropped_columns() {
    let mut assembler = PostgresTransactionAssembler::with_schemas(
        CdcSourceId::new("pg_main").expect("source id"),
        router(),
        orders_schemas(),
        PostgresSchemaEvolutionPolicy::IgnoreCompatible,
    );
    let err = assembler
        .accept_event(xlog(relation_message_with_columns(
            RELATION_ID,
            "orders",
            &[("id", PG_INT8_OID, true)],
        )))
        .expect_err("dropped column should fail");

    assert!(format!("{err:#}").contains("column count decreased"));
}

#[test]
fn schema_policy_rejects_reordered_columns() {
    let mut assembler = PostgresTransactionAssembler::with_schemas(
        CdcSourceId::new("pg_main").expect("source id"),
        router(),
        orders_schemas(),
        PostgresSchemaEvolutionPolicy::IgnoreCompatible,
    );
    let err = assembler
        .accept_event(xlog(relation_message_with_columns(
            RELATION_ID,
            "orders",
            &[("status", PG_TEXT_OID, false), ("id", PG_INT8_OID, true)],
        )))
        .expect_err("reordered columns should fail");

    assert!(format!("{err:#}").contains("column 0 changed"));
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
fn groups_multi_relation_truncate_in_one_source_transaction() {
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
    assembler.accept_event(begin(62)).expect("begin");
    assembler
        .accept_event(xlog(truncate_message([RELATION_ID, OTHER_RELATION_ID])))
        .expect("truncate");

    let transaction = assembler
        .accept_event(commit(45))
        .expect("commit")
        .expect("transaction");
    assert_eq!(transaction.change_batches().len(), 2);
    assert_eq!(
        transaction.change_batches()[0].changes(),
        &[CdcChange::Truncate]
    );
    assert_eq!(
        transaction.change_batches()[1].changes(),
        &[CdcChange::Truncate]
    );
}

#[test]
fn ignores_unrouted_tables_and_empty_transactions() {
    let mut assembler =
        PostgresTransactionAssembler::new(CdcSourceId::new("pg_main").expect("source"), router());
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
    let mut assembler =
        PostgresTransactionAssembler::new(CdcSourceId::new("pg_main").expect("source"), router());
    assembler
        .accept_event(xlog(relation_message(RELATION_ID, "orders")))
        .expect("relation");
    let err = assembler
        .accept_event(xlog(insert_message(RELATION_ID, 7, "open")))
        .expect_err("dml outside transaction should fail");
    assert!(format!("{err:#}").contains("outside a transaction"));
}

#[tokio::test]
async fn applier_returns_feedback_lsn_only_after_table_apply() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let table_store = test_store("pg-cdc-applier-apply").await;
    let mut applier =
        PostgresCdcEventApplier::new(source_id.clone(), table_store.clone(), orders_schemas());

    let relation_outcome = applier
        .accept_event(xlog(relation_message(RELATION_ID, "orders")))
        .await
        .expect("relation");
    assert!(relation_outcome.apply_result().is_none());
    assert_eq!(relation_outcome.feedback_lsn(), None);

    applier.accept_event(begin(58)).await.expect("begin");
    applier
        .accept_event(xlog(insert_message(RELATION_ID, 9, "open")))
        .await
        .expect("insert");
    let outcome = applier
        .accept_event(commit(60))
        .await
        .expect("commit apply");

    assert_eq!(outcome.feedback_lsn(), Some(PostgresLsn::from_u64(60)));
    let apply_result = outcome.apply_result().expect("apply result");
    assert!(!apply_result.already_committed());
    assert_eq!(
        table_store
            .load_checkpoint(&source_id)
            .await
            .expect("load checkpoint"),
        Some(apply_result.checkpoint().clone())
    );
    assert_eq!(
        table_store
            .load_row(
                &CdcTableId::new("orders").expect("table id"),
                &CdcRowKey::new([RowValue::Int64(9)]).expect("key")
            )
            .await
            .expect("load row"),
        Some(
            CdcRow::new([
                Some(RowValue::Int64(9)),
                Some(RowValue::Utf8("open".to_string()))
            ])
            .expect("row")
        )
    );

    let lag = outcome.lag_snapshot();
    assert_eq!(lag.source_id(), &source_id);
    assert_eq!(lag.upstream_wal_end(), Some(PostgresLsn::from_u64(60)));
    assert_eq!(lag.durable_lsn(), Some(PostgresLsn::from_u64(60)));
    assert_eq!(lag.source_lag_bytes(), Some(0));
    assert_eq!(lag.table_lags().len(), 1);
    assert_eq!(
        lag.table_lags()[0].table_id(),
        &CdcTableId::new("orders").expect("table id")
    );
    assert_eq!(
        lag.table_lags()[0].last_applied_lsn(),
        Some(PostgresLsn::from_u64(60))
    );
    assert_eq!(lag.table_lags()[0].table_lag_bytes(), Some(0));
}

#[tokio::test]
async fn applier_does_not_persist_or_feedback_when_table_apply_fails() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let table_store = test_store("pg-cdc-applier-apply-fails").await;
    let mut applier = PostgresCdcEventApplier::with_router(
        source_id.clone(),
        table_store.clone(),
        HashMap::new(),
        router(),
    );

    applier
        .accept_event(xlog(relation_message(RELATION_ID, "orders")))
        .await
        .expect("relation");
    applier.accept_event(begin(59)).await.expect("begin");
    applier
        .accept_event(xlog(insert_message(RELATION_ID, 10, "open")))
        .await
        .expect("insert");
    let err = applier
        .accept_event(commit(70))
        .await
        .expect_err("missing schema should fail apply");
    assert!(format!("{err:#}").contains("unknown table"));
    assert_eq!(
        table_store
            .load_checkpoint(&source_id)
            .await
            .expect("load checkpoint"),
        None
    );
}

#[tokio::test]
async fn apply_loop_ignores_idle_events_and_feedbacks_after_commit() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let table_store = test_store("pg-cdc-loop-feedback").await;
    let mut applier = PostgresCdcEventApplier::new(source_id, table_store, orders_schemas());
    let feedbacks = Arc::new(Mutex::new(Vec::new()));
    let mut stream = FakeStream::new(
        [
            FakeStep::Event(PostgresReplicationEvent::KeepAlive {
                wal_end: PostgresLsn::from_u64(11),
                reply_requested: true,
                server_time_micros: 1,
            }),
            FakeStep::Event(xlog(relation_message(RELATION_ID, "orders"))),
            FakeStep::Event(begin(60)),
            FakeStep::Event(xlog(insert_message(RELATION_ID, 11, "open"))),
            FakeStep::Event(PostgresReplicationEvent::Message {
                transactional: false,
                lsn: PostgresLsn::from_u64(12),
                prefix: "noop".to_string(),
                content: Bytes::new(),
            }),
            FakeStep::Event(commit(80)),
            FakeStep::End,
        ],
        Arc::clone(&feedbacks),
    );

    run_postgres_cdc_apply_loop(&mut stream, &mut applier)
        .await
        .expect("run apply loop");
    assert_eq!(
        *feedbacks.lock().expect("feedback lock"),
        vec![PostgresLsn::from_u64(80)]
    );
}

#[tokio::test]
async fn applier_exposes_shared_source_and_per_table_lag() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let table_store = test_store("pg-cdc-lag-snapshot").await;
    let orders = schema_for(RELATION_ID, "orders", "orders");
    let customers = schema_for(OTHER_RELATION_ID, "customers", "customers");
    let schemas = HashMap::from([
        (orders.table_id().clone(), orders),
        (customers.table_id().clone(), customers),
    ]);
    let mut router = PostgresTableRouter::new();
    router.insert(
        UpstreamTableRef::new("public", "orders").expect("orders upstream"),
        CdcTableId::new("orders").expect("orders id"),
    );
    router.insert(
        UpstreamTableRef::new("public", "customers").expect("customers upstream"),
        CdcTableId::new("customers").expect("customers id"),
    );
    let mut applier =
        PostgresCdcEventApplier::with_router(source_id.clone(), table_store, schemas, router);

    applier
        .accept_event(xlog(relation_message(RELATION_ID, "orders")))
        .await
        .expect("orders relation");
    applier
        .accept_event(xlog(relation_message(OTHER_RELATION_ID, "customers")))
        .await
        .expect("customers relation");
    applier.accept_event(begin(63)).await.expect("begin");
    applier
        .accept_event(xlog(insert_message(RELATION_ID, 20, "open")))
        .await
        .expect("orders insert");
    let applied = applier
        .accept_event(commit(100))
        .await
        .expect("commit apply");

    assert_eq!(applied.lag_snapshot().source_lag_bytes(), Some(0));
    let idle = applier
        .accept_event(PostgresReplicationEvent::KeepAlive {
            wal_end: PostgresLsn::from_u64(150),
            reply_requested: false,
            server_time_micros: 200,
        })
        .await
        .expect("keepalive");
    let lag = idle.lag_snapshot();
    assert_eq!(lag.source_id(), &source_id);
    assert_eq!(lag.upstream_wal_end(), Some(PostgresLsn::from_u64(150)));
    assert_eq!(lag.durable_lsn(), Some(PostgresLsn::from_u64(100)));
    assert_eq!(lag.source_lag_bytes(), Some(50));

    let table_lags = lag.table_lags();
    assert_eq!(table_lags.len(), 2);
    assert_eq!(
        table_lags[0].table_id(),
        &CdcTableId::new("customers").expect("customers id")
    );
    assert_eq!(table_lags[0].last_applied_lsn(), None);
    assert_eq!(table_lags[0].table_lag_bytes(), None);
    assert_eq!(
        table_lags[1].table_id(),
        &CdcTableId::new("orders").expect("orders id")
    );
    assert_eq!(
        table_lags[1].last_applied_lsn(),
        Some(PostgresLsn::from_u64(100))
    );
    assert_eq!(table_lags[1].table_lag_bytes(), Some(50));
}

#[tokio::test]
async fn reconnect_loop_reloads_checkpoint_as_next_start_lsn() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let table_store = test_store("pg-cdc-loop-reconnect").await;
    let mut applier =
        PostgresCdcEventApplier::new(source_id.clone(), table_store.clone(), orders_schemas());
    let feedbacks = Arc::new(Mutex::new(Vec::new()));
    let first_stream = FakeStream::new(
        [
            FakeStep::Event(xlog(relation_message(RELATION_ID, "orders"))),
            FakeStep::Event(begin(61)),
            FakeStep::Event(xlog(insert_message(RELATION_ID, 12, "open"))),
            FakeStep::Event(commit(90)),
            FakeStep::Error("disconnect"),
        ],
        Arc::clone(&feedbacks),
    );
    let second_stream = FakeStream::new([FakeStep::End], Arc::clone(&feedbacks));
    let factory = FakeFactory::new([first_stream, second_stream]);

    run_postgres_cdc_apply_loop_with_reconnect(
        PostgresCdcConfig::new("localhost", "floe", "secret", "app", "slot", "pub")
            .expect("config"),
        &source_id,
        &table_store,
        &mut applier,
        &factory,
        PostgresCdcReconnectPolicy::new(1, Duration::ZERO),
    )
    .await
    .expect("run reconnect loop");

    assert_eq!(
        *feedbacks.lock().expect("feedback lock"),
        vec![PostgresLsn::from_u64(90)]
    );
    let configs = factory.configs();
    assert_eq!(configs.len(), 2);
    assert_eq!(configs[0].start_lsn(), None);
    assert_eq!(configs[1].start_lsn(), Some(PostgresLsn::from_u64(90)));
}

#[tokio::test]
async fn reconnect_loop_replays_inflight_wal_transaction_from_durable_checkpoint() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let table_store = test_store("pg-cdc-loop-inflight-reconnect").await;
    let mut applier =
        PostgresCdcEventApplier::new(source_id.clone(), table_store.clone(), orders_schemas());
    let feedbacks = Arc::new(Mutex::new(Vec::new()));
    let first_stream = FakeStream::new(
        [
            FakeStep::Event(xlog(relation_message(RELATION_ID, "orders"))),
            FakeStep::Event(begin(61)),
            FakeStep::Event(xlog(insert_message(RELATION_ID, 12, "committed"))),
            FakeStep::Event(commit(90)),
            FakeStep::Event(begin(63)),
            FakeStep::Event(xlog(insert_message(RELATION_ID, 14, "inflight"))),
            FakeStep::Error("disconnect before commit"),
        ],
        Arc::clone(&feedbacks),
    );
    let second_stream = FakeStream::new(
        [
            FakeStep::Event(xlog(relation_message(RELATION_ID, "orders"))),
            FakeStep::Event(begin(63)),
            FakeStep::Event(xlog(insert_message(RELATION_ID, 14, "replayed"))),
            FakeStep::Event(commit(120)),
            FakeStep::End,
        ],
        Arc::clone(&feedbacks),
    );
    let factory = FakeFactory::new([first_stream, second_stream]);

    run_postgres_cdc_apply_loop_with_reconnect(
        PostgresCdcConfig::new("localhost", "floe", "secret", "app", "slot", "pub")
            .expect("config"),
        &source_id,
        &table_store,
        &mut applier,
        &factory,
        PostgresCdcReconnectPolicy::new(1, Duration::ZERO),
    )
    .await
    .expect("run reconnect loop");

    assert_eq!(
        *feedbacks.lock().expect("feedback lock"),
        vec![PostgresLsn::from_u64(90), PostgresLsn::from_u64(120)]
    );
    let configs = factory.configs();
    assert_eq!(configs.len(), 2);
    assert_eq!(configs[0].start_lsn(), None);
    assert_eq!(configs[1].start_lsn(), Some(PostgresLsn::from_u64(90)));
    let checkpoint = table_store
        .load_checkpoint(&source_id)
        .await
        .expect("load checkpoint")
        .expect("checkpoint");
    assert_eq!(
        PostgresLsn::from_source_position(checkpoint.position()).expect("checkpoint lsn"),
        PostgresLsn::from_u64(120)
    );
    assert_eq!(
        table_store
            .load_row(
                &CdcTableId::new("orders").expect("table id"),
                &CdcRowKey::new([RowValue::Int64(12)]).expect("key")
            )
            .await
            .expect("load committed row"),
        Some(
            CdcRow::new([
                Some(RowValue::Int64(12)),
                Some(RowValue::Utf8("committed".to_string())),
            ])
            .expect("row")
        )
    );
    assert_eq!(
        table_store
            .load_row(
                &CdcTableId::new("orders").expect("table id"),
                &CdcRowKey::new([RowValue::Int64(14)]).expect("key")
            )
            .await
            .expect("load replayed row"),
        Some(
            CdcRow::new([
                Some(RowValue::Int64(14)),
                Some(RowValue::Utf8("replayed".to_string())),
            ])
            .expect("row")
        )
    );
}

#[tokio::test]
async fn reconnect_loop_resumes_after_compatible_schema_change() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let table_store = test_store("pg-cdc-loop-schema-reconnect").await;
    let relation = relation_message_with_columns(
        RELATION_ID,
        "orders",
        &[
            ("id", PG_INT8_OID, true),
            ("status", PG_TEXT_OID, false),
            ("note", PG_TEXT_OID, false),
        ],
    );
    let PgOutputMessage::Relation(observed_relation) =
        decode_pgoutput_message(relation.clone()).expect("decode relation")
    else {
        panic!("expected relation");
    };
    let observed_schema = observed_relation
        .to_cdc_schema(CdcTableId::new("orders").expect("table id"))
        .expect("observed schema");
    let mut applier = PostgresCdcEventApplier::with_schema_policy(
        source_id.clone(),
        table_store.clone(),
        orders_schemas(),
        PostgresSchemaEvolutionPolicy::IgnoreCompatible,
    );
    let feedbacks = Arc::new(Mutex::new(Vec::new()));
    let first_stream = FakeStream::new(
        [
            FakeStep::Event(xlog(relation)),
            FakeStep::Event(begin(62)),
            FakeStep::Event(xlog(insert_message_with_values(
                RELATION_ID,
                &["13".to_string(), "open".to_string(), "ignored".to_string()],
            ))),
            FakeStep::Event(commit(120)),
            FakeStep::Error("disconnect after schema change"),
        ],
        Arc::clone(&feedbacks),
    );
    let second_stream = FakeStream::new([FakeStep::End], Arc::clone(&feedbacks));
    let factory = FakeFactory::new([first_stream, second_stream]);

    run_postgres_cdc_apply_loop_with_reconnect(
        PostgresCdcConfig::new("localhost", "floe", "secret", "app", "slot", "pub")
            .expect("config"),
        &source_id,
        &table_store,
        &mut applier,
        &factory,
        PostgresCdcReconnectPolicy::new(1, Duration::ZERO),
    )
    .await
    .expect("run reconnect loop");

    assert_eq!(
        *feedbacks.lock().expect("feedback lock"),
        vec![PostgresLsn::from_u64(120)]
    );
    let configs = factory.configs();
    assert_eq!(configs.len(), 2);
    assert_eq!(configs[0].start_lsn(), None);
    assert_eq!(configs[1].start_lsn(), Some(PostgresLsn::from_u64(120)));
    let checkpoint = table_store
        .load_checkpoint(&source_id)
        .await
        .expect("load checkpoint")
        .expect("checkpoint");
    assert_eq!(
        checkpoint.schema_versions().get("orders").copied(),
        Some(observed_schema.stable_fingerprint())
    );
    assert_eq!(
        table_store
            .load_row(
                &CdcTableId::new("orders").expect("table id"),
                &CdcRowKey::new([RowValue::Int64(13)]).expect("key")
            )
            .await
            .expect("load row"),
        Some(
            CdcRow::new([
                Some(RowValue::Int64(13)),
                Some(RowValue::Utf8("open".to_string())),
            ])
            .expect("row")
        )
    );
}

#[tokio::test]
async fn reconnect_loop_errors_after_max_reconnects() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let table_store = test_store("pg-cdc-loop-reconnect-exhausted").await;
    let mut applier =
        PostgresCdcEventApplier::new(source_id.clone(), table_store.clone(), orders_schemas());
    let feedbacks = Arc::new(Mutex::new(Vec::new()));
    let factory = FakeFactory::new([
        FakeStream::new([FakeStep::Error("disconnect 1")], Arc::clone(&feedbacks)),
        FakeStream::new([FakeStep::Error("disconnect 2")], feedbacks),
    ]);

    let err = run_postgres_cdc_apply_loop_with_reconnect(
        PostgresCdcConfig::new("localhost", "floe", "secret", "app", "slot", "pub")
            .expect("config"),
        &source_id,
        &table_store,
        &mut applier,
        &factory,
        PostgresCdcReconnectPolicy::new(1, Duration::ZERO),
    )
    .await
    .expect_err("reconnects should be exhausted");
    assert!(format!("{err:#}").contains("failed after 1 reconnect"));
}
