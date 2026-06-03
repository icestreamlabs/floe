use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use dbsp_storage::storage::{KeyValueTable, SlateTable};
use floe_cdc::CdcTableStore;
use floe_cdc_core::{
    CdcColumn, CdcPrimaryKey, CdcRow, CdcRowKey, CdcSourceId, CdcTableId, CdcTableSchema,
    UpstreamTableRef,
};
use floe_cdc_pg::{
    PostgresCdcConfig, PostgresCdcEventApplier, PostgresLsn, PostgresReplicationClient,
};
use floe_core::RowValue;
use floe_core::catalog::ColumnType;
use object_store::memory::InMemory;
use slatedb::Db;
use tokio::time::{Instant, interval, timeout};
use tokio_postgres::NoTls;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
enum ConformanceArea {
    SnapshotHandoff,
    MultiTransactionOrdering,
    MultiRowTransactionOrder,
    RestartCheckpointProgression,
    ToastUnchangedValues,
    DeleteUpdatePrimaryKey,
    ReplicaIdentity,
    CoreTypeMapping,
    DebeziumEnvelope,
    UnsupportedCases,
}

struct ConformanceCase {
    id: &'static str,
    areas: &'static [ConformanceArea],
    evidence: &'static [&'static str],
}

const POSTGRES_CDC_CONFORMANCE_MATRIX: &[ConformanceCase] = &[
    ConformanceCase {
        id: "snapshot_stream_handoff",
        areas: &[
            ConformanceArea::SnapshotHandoff,
            ConformanceArea::MultiTransactionOrdering,
        ],
        evidence: &[
            "postgres_cdc_shared_source_snapshot_converges_to_wal_stream",
            "postgres_cdc_sql_source_table_snapshot_backfill_acceptance",
            "postgres_pgoutput_stream_updates_cdc_table_state",
        ],
    },
    ConformanceCase {
        id: "wal_transaction_order",
        areas: &[
            ConformanceArea::MultiTransactionOrdering,
            ConformanceArea::MultiRowTransactionOrder,
        ],
        evidence: &[
            "preserves_multi_row_order_within_one_source_transaction",
            "groups_multiple_tables_in_one_source_transaction",
            "applier_returns_feedback_lsn_only_after_table_apply",
        ],
    },
    ConformanceCase {
        id: "restart_checkpoint_progression",
        areas: &[ConformanceArea::RestartCheckpointProgression],
        evidence: &[
            "postgres_cdc_table_restart_resumes_from_committed_lsn",
            "reconnect_loop_reloads_checkpoint_as_next_start_lsn",
            "reconnect_loop_replays_inflight_wal_transaction_from_durable_checkpoint",
        ],
    },
    ConformanceCase {
        id: "toast_partial_rows",
        areas: &[ConformanceArea::ToastUnchangedValues],
        evidence: &[
            "postgres_pgoutput_completes_unchanged_toast_values",
            "decodes_update_with_unchanged_toast_marker",
            "resolves_unchanged_toast_columns_from_previous_row",
        ],
    },
    ConformanceCase {
        id: "delete_update_primary_key",
        areas: &[ConformanceArea::DeleteUpdatePrimaryKey],
        evidence: &[
            "postgres_pgoutput_stream_updates_cdc_table_state",
            "applier_moves_primary_key_updates_between_keys",
            "applies_insert_update_and_delete_with_atomic_checkpoint",
        ],
    },
    ConformanceCase {
        id: "replica_identity",
        areas: &[ConformanceArea::ReplicaIdentity],
        evidence: &[
            "decodes_replica_identity_modes_and_reports_unsupported_identity",
            "decodes_replica_identity_full_update_with_before_image",
            "validate_upstream_table_schema rejects REPLICA IDENTITY NOTHING",
        ],
    },
    ConformanceCase {
        id: "core_type_mapping",
        areas: &[ConformanceArea::CoreTypeMapping],
        evidence: &[
            "pgoutput_type_compatibility_matrix_is_explicit",
            "postgres_cdc_type_mapping_covers_claimed_common_types",
            "postgres_cdc_type_coverage_to_postgres_sink_acceptance",
        ],
    },
    ConformanceCase {
        id: "debezium_envelope",
        areas: &[ConformanceArea::DebeziumEnvelope],
        evidence: &[
            "pipeline_debezium_records_validate_actual_kafka_shape",
            "envelope_payload_exposes_debezium_compatibility_fields",
            "pipeline_debezium_records_are_buffered_as_encoded_kafka_payloads",
        ],
    },
    ConformanceCase {
        id: "unsupported_cases",
        areas: &[ConformanceArea::UnsupportedCases],
        evidence: &[
            "schema_policy_rejects_incompatible_type_changes",
            "schema_policy_rejects_dropped_columns",
            "truncate_is_rejected_without_mutating_checkpoint",
            "pgoutput_type_compatibility_matrix_is_explicit",
        ],
    },
];

#[test]
fn postgres_cdc_conformance_matrix_tracks_required_semantics() {
    let mut ids = HashSet::new();
    for case in POSTGRES_CDC_CONFORMANCE_MATRIX {
        assert!(
            ids.insert(case.id),
            "duplicate conformance case {}",
            case.id
        );
        assert!(!case.areas.is_empty(), "case {} has no areas", case.id);
        assert!(
            !case.evidence.is_empty(),
            "case {} has no test evidence",
            case.id
        );
    }

    for required in [
        ConformanceArea::SnapshotHandoff,
        ConformanceArea::MultiTransactionOrdering,
        ConformanceArea::MultiRowTransactionOrder,
        ConformanceArea::RestartCheckpointProgression,
        ConformanceArea::ToastUnchangedValues,
        ConformanceArea::DeleteUpdatePrimaryKey,
        ConformanceArea::ReplicaIdentity,
        ConformanceArea::CoreTypeMapping,
        ConformanceArea::DebeziumEnvelope,
        ConformanceArea::UnsupportedCases,
    ] {
        assert!(
            POSTGRES_CDC_CONFORMANCE_MATRIX
                .iter()
                .any(|case| case.areas.contains(&required)),
            "conformance matrix is missing {:?}",
            required
        );
    }
}

#[tokio::test]
#[ignore = "requires logical-replication Postgres; run scripts/run_postgres_cdc_pgoutput_e2e.sh"]
async fn postgres_pgoutput_stream_updates_cdc_table_state() -> Result<()> {
    let env = PgEnv::from_env()?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let table_name = format!("floe_cdc_orders_{run_id}");
    let slot = format!("floe_cdc_slot_{run_id}");
    let publication = format!("floe_cdc_pub_{run_id}");
    let source_id = CdcSourceId::new("pg_main")?;
    let table_id = CdcTableId::new("orders")?;
    let schema = orders_schema(table_id.clone(), &table_name)?;
    let store = test_store(&format!("pgoutput-e2e-{run_id}")).await?;

    let (client, connection) = tokio_postgres::connect(&env.dsn(), NoTls)
        .await
        .context("connect Postgres control plane")?;
    let _connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    setup_publication_and_slot(&client, &table_name, &publication, &slot).await?;
    let start_lsn = create_slot(&client, &slot).await?;

    let config = PostgresCdcConfig::new(
        env.host.clone(),
        env.user.clone(),
        env.password.clone(),
        env.database.clone(),
        slot.clone(),
        publication.clone(),
    )?
    .with_port(env.port)?
    .with_start_lsn(start_lsn)
    .with_status_interval(Duration::from_millis(100))?
    .with_idle_wakeup_interval(Duration::from_secs(1))?;
    let mut replication = PostgresReplicationClient::connect(&config).await?;
    let mut applier = PostgresCdcEventApplier::new(
        source_id.clone(),
        store.clone(),
        HashMap::from([(table_id.clone(), schema)]),
    );

    let test_result = async {
        client
            .execute(
                &format!("INSERT INTO {table_name} (id, amount, note) VALUES ($1, $2, $3)"),
                &[&1_i64, &100_i64, &"open"],
            )
            .await
            .context("insert source row")?;
        process_until_row(
            &store,
            &mut replication,
            &mut applier,
            &table_id,
            key(1),
            Some(row(1, 100, "open")?),
            "inserted row",
        )
        .await?;

        client
            .execute(
                &format!("UPDATE {table_name} SET amount = $1, note = $2 WHERE id = $3"),
                &[&150_i64, &"paid", &1_i64],
            )
            .await
            .context("update source row")?;
        process_until_row(
            &store,
            &mut replication,
            &mut applier,
            &table_id,
            key(1),
            Some(row(1, 150, "paid")?),
            "updated row",
        )
        .await?;

        client
            .execute(
                &format!("DELETE FROM {table_name} WHERE id = $1"),
                &[&1_i64],
            )
            .await
            .context("delete source row")?;
        process_until_row(
            &store,
            &mut replication,
            &mut applier,
            &table_id,
            key(1),
            None,
            "deleted row",
        )
        .await?;

        let checkpoint = store
            .load_checkpoint(&source_id)
            .await
            .context("load final CDC checkpoint")?
            .context("expected final CDC checkpoint")?;
        let durable_lsn = PostgresLsn::from_source_position(checkpoint.position())?;
        let lag = applier.lag_snapshot();
        anyhow::ensure!(
            lag.durable_lsn() == Some(durable_lsn),
            "lag durable LSN {:?} did not match checkpoint LSN {durable_lsn}",
            lag.durable_lsn()
        );
        anyhow::ensure!(
            lag.table_lags()
                .iter()
                .any(|table| table.table_id() == &table_id && table.last_applied_lsn().is_some()),
            "expected table lag snapshot to include an applied LSN"
        );
        Ok::<(), anyhow::Error>(())
    }
    .await;

    replication.stop();
    let _ = replication.shutdown().await;
    cleanup_postgres(&client, &publication, &slot, &table_name).await;
    test_result
}

#[tokio::test]
#[ignore = "requires logical-replication Postgres; run scripts/run_postgres_cdc_pgoutput_e2e.sh"]
async fn postgres_pgoutput_completes_unchanged_toast_values() -> Result<()> {
    let env = PgEnv::from_env()?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let table_name = format!("floe_cdc_toast_orders_{run_id}");
    let slot = format!("floe_cdc_toast_slot_{run_id}");
    let publication = format!("floe_cdc_toast_pub_{run_id}");
    let source_id = CdcSourceId::new("pg_toast")?;
    let table_id = CdcTableId::new("orders")?;
    let schema = orders_schema(table_id.clone(), &table_name)?;
    let store = test_store(&format!("pgoutput-toast-e2e-{run_id}")).await?;

    let (client, connection) = tokio_postgres::connect(&env.dsn(), NoTls)
        .await
        .context("connect Postgres control plane for TOAST test")?;
    let _connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    setup_publication_and_slot(&client, &table_name, &publication, &slot).await?;
    client
        .batch_execute(&format!(
            "ALTER TABLE {table_name} ALTER COLUMN note SET STORAGE EXTERNAL;"
        ))
        .await
        .context("force source note column into external TOAST storage")?;
    let start_lsn = create_slot(&client, &slot).await?;

    let config = PostgresCdcConfig::new(
        env.host.clone(),
        env.user.clone(),
        env.password.clone(),
        env.database.clone(),
        slot.clone(),
        publication.clone(),
    )?
    .with_port(env.port)?
    .with_start_lsn(start_lsn)
    .with_status_interval(Duration::from_millis(100))?
    .with_idle_wakeup_interval(Duration::from_secs(1))?;
    let mut replication = PostgresReplicationClient::connect(&config).await?;
    let mut applier = PostgresCdcEventApplier::new(
        source_id,
        store.clone(),
        HashMap::from([(table_id.clone(), schema)]),
    );

    let test_result = async {
        let large_note = large_toast_note();
        client
            .execute(
                &format!("INSERT INTO {table_name} (id, amount, note) VALUES ($1, $2, $3)"),
                &[&11_i64, &110_i64, &large_note],
            )
            .await
            .context("insert source row with TOAST-sized text")?;
        process_until_row(
            &store,
            &mut replication,
            &mut applier,
            &table_id,
            key(11),
            Some(row(11, 110, &large_note)?),
            "inserted TOAST row",
        )
        .await?;

        client
            .execute(
                &format!("UPDATE {table_name} SET amount = $1 WHERE id = $2"),
                &[&111_i64, &11_i64],
            )
            .await
            .context("update source row without changing TOAST column")?;
        process_until_row(
            &store,
            &mut replication,
            &mut applier,
            &table_id,
            key(11),
            Some(row(11, 111, &large_note)?),
            "updated TOAST row",
        )
        .await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    replication.stop();
    let _ = replication.shutdown().await;
    cleanup_postgres(&client, &publication, &slot, &table_name).await;
    test_result
}

#[tokio::test]
#[ignore = "requires logical-replication Postgres; run scripts/run_postgres_cdc_pgoutput_e2e.sh"]
async fn postgres_pgoutput_survives_idle_before_wal() -> Result<()> {
    let env = PgEnv::from_env()?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let table_name = format!("floe_cdc_idle_orders_{run_id}");
    let slot = format!("floe_cdc_idle_slot_{run_id}");
    let publication = format!("floe_cdc_idle_pub_{run_id}");
    let source_id = CdcSourceId::new("pg_idle")?;
    let table_id = CdcTableId::new("orders")?;
    let schema = orders_schema(table_id.clone(), &table_name)?;
    let store = test_store(&format!("pgoutput-idle-e2e-{run_id}")).await?;

    let (client, connection) = tokio_postgres::connect(&env.dsn(), NoTls)
        .await
        .context("connect Postgres control plane for idle test")?;
    let _connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    setup_publication_and_slot(&client, &table_name, &publication, &slot).await?;
    let start_lsn = create_slot(&client, &slot).await?;

    let config = PostgresCdcConfig::new(
        env.host.clone(),
        env.user.clone(),
        env.password.clone(),
        env.database.clone(),
        slot.clone(),
        publication.clone(),
    )?
    .with_port(env.port)?
    .with_start_lsn(start_lsn)
    .with_status_interval(Duration::from_millis(100))?
    .with_idle_wakeup_interval(Duration::from_millis(200))?;
    let mut replication = PostgresReplicationClient::connect(&config).await?;
    let mut applier = PostgresCdcEventApplier::new(
        source_id.clone(),
        store.clone(),
        HashMap::from([(table_id.clone(), schema)]),
    );

    let test_result = async {
        let idle_deadline = Instant::now() + Duration::from_millis(700);
        let mut idle_poll = interval(Duration::from_millis(100));
        while Instant::now() < idle_deadline {
            anyhow::ensure!(
                store.load_checkpoint(&source_id).await?.is_none(),
                "idle replication should not create a durable checkpoint before WAL data"
            );
            anyhow::ensure!(
                replication.is_running(),
                "replication client should still be running after an idle interval"
            );
            idle_poll.tick().await;
        }

        client
            .execute(
                &format!("INSERT INTO {table_name} (id, amount, note) VALUES ($1, $2, $3)"),
                &[&7_i64, &700_i64, &"after_idle"],
            )
            .await
            .context("insert source row after idle interval")?;
        process_until_row(
            &store,
            &mut replication,
            &mut applier,
            &table_id,
            key(7),
            Some(row(7, 700, "after_idle")?),
            "row after idle",
        )
        .await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    replication.stop();
    let _ = replication.shutdown().await;
    cleanup_postgres(&client, &publication, &slot, &table_name).await;
    test_result
}

#[derive(Debug)]
struct PgEnv {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
}

impl PgEnv {
    fn from_env() -> Result<Self> {
        Ok(Self {
            host: std::env::var("FLOE_CDC_PG_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
            port: std::env::var("FLOE_CDC_PG_PORT")
                .unwrap_or_else(|_| "55432".to_string())
                .parse()
                .context("parse FLOE_CDC_PG_PORT")?,
            user: std::env::var("FLOE_CDC_PG_USER").unwrap_or_else(|_| "postgres".to_string()),
            password: std::env::var("FLOE_CDC_PG_PASSWORD")
                .unwrap_or_else(|_| "postgres".to_string()),
            database: std::env::var("FLOE_CDC_PG_DATABASE")
                .unwrap_or_else(|_| "postgres".to_string()),
        })
    }

    fn dsn(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.user, self.password, self.database
        )
    }
}

async fn test_store(name: &str) -> Result<CdcTableStore> {
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open(name, object_store)
            .await
            .context("open SlateDB test store")?,
    );
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
    Ok(CdcTableStore::new(table))
}

fn orders_schema(table_id: CdcTableId, table_name: &str) -> Result<CdcTableSchema> {
    CdcTableSchema::new(
        table_id,
        UpstreamTableRef::new("public", table_name)?,
        vec![
            CdcColumn::new("id", ColumnType::Int64, false)?,
            CdcColumn::new("amount", ColumnType::Int64, false)?,
            CdcColumn::new("note", ColumnType::Utf8, true)?,
        ],
        CdcPrimaryKey::new(["id"])?,
    )
}

fn key(id: i64) -> CdcRowKey {
    CdcRowKey::new([RowValue::Int64(id)]).expect("valid test row key")
}

fn row(id: i64, amount: i64, note: &str) -> Result<CdcRow> {
    CdcRow::new([
        Some(RowValue::Int64(id)),
        Some(RowValue::Int64(amount)),
        Some(RowValue::Utf8(note.to_string())),
    ])
}

fn large_toast_note() -> String {
    (0..2048)
        .map(|idx| format!("toast-segment-{idx:04}-{}", "x".repeat(48)))
        .collect::<Vec<_>>()
        .join("|")
}

async fn setup_publication_and_slot(
    client: &tokio_postgres::Client,
    table_name: &str,
    publication: &str,
    slot: &str,
) -> Result<()> {
    cleanup_postgres(client, publication, slot, table_name).await;
    client
        .batch_execute(&format!(
            "CREATE TABLE {table_name} (
               id BIGINT PRIMARY KEY,
               amount BIGINT NOT NULL,
               note TEXT
             );
             CREATE PUBLICATION {publication} FOR TABLE {table_name};"
        ))
        .await
        .context("create source table and publication")
}

async fn create_slot(client: &tokio_postgres::Client, slot: &str) -> Result<PostgresLsn> {
    let row = client
        .query_one(
            "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await
        .context("create pgoutput logical replication slot")?;
    let lsn: String = row.get(0);
    PostgresLsn::parse(&lsn)
}

async fn cleanup_postgres(
    client: &tokio_postgres::Client,
    publication: &str,
    slot: &str,
    table_name: &str,
) {
    let _ = client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication};"))
        .await;
    let _ = client
        .execute(
            "SELECT pg_drop_replication_slot($1)
             WHERE EXISTS (
               SELECT 1
               FROM pg_replication_slots
               WHERE slot_name = $1
             )",
            &[&slot],
        )
        .await;
    let _ = client
        .batch_execute(&format!("DROP TABLE IF EXISTS {table_name};"))
        .await;
}

async fn process_until_row(
    store: &CdcTableStore,
    replication: &mut PostgresReplicationClient,
    applier: &mut PostgresCdcEventApplier,
    table_id: &CdcTableId,
    key: CdcRowKey,
    expected: Option<CdcRow>,
    label: &str,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if store
            .load_row(table_id, &key)
            .await
            .with_context(|| format!("load CDC row while waiting for {label}"))?
            == expected
        {
            return Ok(());
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            bail!("timed out waiting for {label}");
        }
        let event = timeout(remaining.min(Duration::from_secs(2)), replication.recv())
            .await
            .with_context(|| format!("timed out receiving replication event for {label}"))?
            .with_context(|| format!("receive replication event for {label}"))?
            .with_context(|| format!("replication stream ended before {label}"))?;
        let outcome = applier.accept_event(event).await?;
        if let Some(feedback_lsn) = outcome.feedback_lsn() {
            replication.update_applied_lsn(feedback_lsn);
        }
    }
}
