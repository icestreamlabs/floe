use super::*;

use anyhow::{Context, Result, anyhow, bail, ensure};
use floe_cdc_core::{
    CdcCheckpoint, CdcColumnarColumn, CdcColumnarRowBatch, CdcTransactionId, ChangeBatch,
    TransactionBatch,
};
use futures::{StreamExt, TryStreamExt, pin_mut};
use std::sync::LazyLock;
use std::time::Instant;
use tokio_postgres::types::ToSql;
use tokio_postgres::types::Type;

const DEFAULT_POSTGRES_SNAPSHOT_ROWS_PER_BATCH: usize = 16_384;
const DEFAULT_POSTGRES_SNAPSHOT_MAX_WORKERS: usize = 1;
static POSTGRES_SNAPSHOT_ROWS_PER_BATCH: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_POSTGRES_SNAPSHOT_ROWS_PER_BATCH)
});
static POSTGRES_SNAPSHOT_MAX_WORKERS: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_POSTGRES_SNAPSHOT_MAX_WORKERS)
});
static CDC_PERF_LOGGING_ENABLED: LazyLock<bool> = LazyLock::new(|| {
    std::env::var("FLOE_CDC_PERF_LOG")
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
});

struct PostgresSnapshot {
    lsn: PostgresLsn,
    transaction: Option<TransactionBatch>,
    row_count: usize,
}

pub(super) async fn ensure_postgres_cdc_publication_and_slot(
    connection_string: &str,
    slot: &str,
    publication: &str,
    runtime_plan: &PostgresCdcRuntimePlan,
) -> Result<()> {
    let (client, connection) = tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
        .await
        .context("connect Postgres control plane for CDC setup")?;
    let connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::debug!(error = %err, "Postgres CDC setup connection closed");
        }
    });

    let setup_result = ensure_postgres_cdc_publication_and_slot_with_client(
        &client,
        slot,
        publication,
        runtime_plan,
    )
    .await;
    drop(client);
    connection_task.abort();
    setup_result
}

async fn ensure_postgres_cdc_publication_and_slot_with_client(
    client: &tokio_postgres::Client,
    slot: &str,
    publication: &str,
    runtime_plan: &PostgresCdcRuntimePlan,
) -> Result<()> {
    let publication_exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&publication],
        )
        .await
        .with_context(|| format!("check Postgres CDC publication '{publication}'"))?
        .get(0);
    if !publication_exists {
        let schemas = sorted_snapshot_schemas(&runtime_plan.schemas);
        ensure!(
            !schemas.is_empty(),
            "cannot create Postgres CDC publication '{publication}' without tables"
        );
        let mut tables = schemas
            .iter()
            .map(|schema| qualified_table_name(schema.upstream_table()))
            .collect::<Vec<_>>();
        tables.sort();
        tables.dedup();
        client
            .batch_execute(&format!(
                "CREATE PUBLICATION {} FOR TABLE {}",
                quote_pg_ident(publication),
                tables.join(", ")
            ))
            .await
            .with_context(|| format!("create Postgres CDC publication '{publication}'"))?;
        tracing::info!(
            source = %runtime_plan.source_id.as_str(),
            publication = %publication,
            tables = tables.len(),
            "created Postgres CDC publication"
        );
    }

    let slot_row = client
        .query_opt(
            "SELECT plugin
             FROM pg_replication_slots
             WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .with_context(|| format!("check Postgres CDC logical replication slot '{slot}'"))?;
    match slot_row {
        Some(row) => {
            let plugin: Option<String> = row.get(0);
            ensure!(
                plugin.as_deref() == Some("pgoutput"),
                "Postgres CDC logical replication slot '{slot}' must use pgoutput, got {:?}",
                plugin
            );
        }
        None => {
            client
                .query_one(
                    "SELECT * FROM pg_create_logical_replication_slot($1, 'pgoutput')",
                    &[&slot],
                )
                .await
                .with_context(|| {
                    format!("create Postgres CDC pgoutput replication slot '{slot}'")
                })?;
            tracing::info!(
                source = %runtime_plan.source_id.as_str(),
                slot = %slot,
                "created Postgres CDC logical replication slot"
            );
        }
    }

    Ok(())
}

pub(super) async fn run_initial_postgres_snapshot_if_needed(
    connection_string: &str,
    slot: &str,
    publication: &str,
    runtime_plan: &PostgresCdcRuntimePlan,
    table_store: &CdcTableStore,
    sender: &mpsc::Sender<QueuedCdcTransaction>,
    commit_lsn_rx: Option<&mut watch::Receiver<PostgresCdcCommit>>,
    cancel: &CancellationToken,
) -> Result<Option<PostgresLsn>> {
    if table_store
        .load_checkpoint(&runtime_plan.source_id)
        .await
        .with_context(|| {
            format!(
                "load CDC checkpoint before Postgres snapshot for '{}'",
                runtime_plan.source_id.as_str()
            )
        })?
        .is_some()
    {
        return Ok(None);
    }

    let snapshot =
        load_postgres_initial_snapshot(connection_string, publication, runtime_plan).await?;
    finish_loaded_postgres_snapshot(
        slot,
        publication,
        runtime_plan,
        table_store,
        sender,
        commit_lsn_rx,
        cancel,
        snapshot,
    )
    .await
}

async fn finish_loaded_postgres_snapshot(
    slot: &str,
    publication: &str,
    runtime_plan: &PostgresCdcRuntimePlan,
    table_store: &CdcTableStore,
    sender: &mpsc::Sender<QueuedCdcTransaction>,
    commit_lsn_rx: Option<&mut watch::Receiver<PostgresCdcCommit>>,
    cancel: &CancellationToken,
    snapshot: PostgresSnapshot,
) -> Result<Option<PostgresLsn>> {
    match snapshot.transaction {
        Some(transaction) => {
            sender
                .send(QueuedCdcTransaction {
                    slot: slot.to_string(),
                    source_id: runtime_plan.source_id.clone(),
                    transaction,
                })
                .await
                .map_err(|err| anyhow!("failed to enqueue initial Postgres CDC snapshot: {err}"))?;
            wait_for_postgres_snapshot_commit(commit_lsn_rx, slot, snapshot.lsn, cancel).await?;
        }
        None => {
            let checkpoint = snapshot_checkpoint(&runtime_plan.source_id, snapshot.lsn)?;
            table_store.commit_checkpoint(&checkpoint).await?;
        }
    }

    tracing::info!(
        source = %runtime_plan.source_id.as_str(),
        slot = %slot,
        publication = %publication,
        lsn = %snapshot.lsn,
        rows = snapshot.row_count,
        "completed initial Postgres CDC snapshot"
    );
    Ok(Some(snapshot.lsn))
}

async fn load_postgres_initial_snapshot(
    connection_string: &str,
    publication: &str,
    runtime_plan: &PostgresCdcRuntimePlan,
) -> Result<PostgresSnapshot> {
    let (mut client, connection) =
        tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
            .await
            .context("connect Postgres control plane for initial CDC snapshot")?;
    let connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::debug!(error = %err, "Postgres initial snapshot connection closed");
        }
    });

    let snapshot = load_postgres_initial_snapshot_from_client(
        connection_string,
        &mut client,
        publication,
        &runtime_plan.source_id,
        &runtime_plan.schemas,
    )
    .await;
    drop(client);
    connection_task.abort();
    snapshot
}

async fn load_postgres_initial_snapshot_from_client(
    connection_string: &str,
    client: &mut tokio_postgres::Client,
    publication: &str,
    source_id: &CdcSourceId,
    schemas: &HashMap<CdcTableId, CdcTableSchema>,
) -> Result<PostgresSnapshot> {
    let sorted_schemas = sorted_snapshot_schemas(schemas);
    let max_workers = *POSTGRES_SNAPSHOT_MAX_WORKERS;
    if max_workers > 1 && sorted_schemas.len() > 1 {
        return load_parallel_postgres_initial_snapshot_from_client(
            connection_string,
            client,
            publication,
            source_id,
            sorted_schemas,
            max_workers,
        )
        .await;
    }

    let transaction = client
        .transaction()
        .await
        .context("begin initial Postgres CDC snapshot transaction")?;

    if !sorted_schemas.is_empty() {
        transaction
            .batch_execute(&snapshot_lock_sql(&sorted_schemas))
            .await
            .context("lock Postgres CDC snapshot tables")?;
    }

    validate_publication_tables(&transaction, publication, &sorted_schemas).await?;
    for schema in &sorted_schemas {
        validate_upstream_table_schema(&transaction, schema).await?;
    }

    let lsn_row = transaction
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .context("capture Postgres CDC snapshot LSN")?;
    let lsn_text: String = lsn_row.get(0);
    let snapshot_lsn = PostgresLsn::parse(&lsn_text)?;

    let mut change_batches = Vec::new();
    let mut row_count = 0_usize;
    for schema in sorted_schemas {
        let table_snapshot = snapshot_table_change_batches(&transaction, schema).await?;
        row_count = row_count.saturating_add(table_snapshot.row_count);
        change_batches.extend(table_snapshot.change_batches);
    }
    transaction
        .commit()
        .await
        .context("commit initial Postgres CDC snapshot transaction")?;

    let transaction = snapshot_transaction_batch(source_id, snapshot_lsn, change_batches)?;

    Ok(PostgresSnapshot {
        lsn: snapshot_lsn,
        transaction,
        row_count,
    })
}

async fn load_parallel_postgres_initial_snapshot_from_client(
    connection_string: &str,
    client: &mut tokio_postgres::Client,
    publication: &str,
    source_id: &CdcSourceId,
    sorted_schemas: Vec<&CdcTableSchema>,
    max_workers: usize,
) -> Result<PostgresSnapshot> {
    let transaction = client
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .context("begin exported initial Postgres CDC snapshot transaction")?;

    if !sorted_schemas.is_empty() {
        transaction
            .batch_execute(&snapshot_lock_sql(&sorted_schemas))
            .await
            .context("lock Postgres CDC snapshot tables")?;
    }

    validate_publication_tables(&transaction, publication, &sorted_schemas).await?;
    for schema in &sorted_schemas {
        validate_upstream_table_schema(&transaction, schema).await?;
    }

    let lsn_row = transaction
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .context("capture Postgres CDC snapshot LSN")?;
    let lsn_text: String = lsn_row.get(0);
    let snapshot_lsn = PostgresLsn::parse(&lsn_text)?;
    let exported_snapshot_row = transaction
        .query_one("SELECT pg_export_snapshot()", &[])
        .await
        .context("export Postgres CDC snapshot for parallel table reads")?;
    let exported_snapshot: String = exported_snapshot_row.get(0);

    let schemas = sorted_schemas.into_iter().cloned().collect::<Vec<_>>();
    let mut table_snapshots =
        futures::stream::iter(schemas.into_iter().enumerate().map(|(idx, schema)| {
            let connection_string = connection_string.to_string();
            let exported_snapshot = exported_snapshot.clone();
            async move {
                snapshot_table_change_batches_from_exported_snapshot(
                    &connection_string,
                    &exported_snapshot,
                    &schema,
                )
                .await
                .map(|snapshot| (idx, snapshot))
            }
        }))
        .buffer_unordered(max_workers)
        .try_collect::<Vec<_>>()
        .await?;
    table_snapshots.sort_by_key(|(idx, _)| *idx);

    transaction
        .commit()
        .await
        .context("commit exported initial Postgres CDC snapshot transaction")?;

    let mut change_batches = Vec::new();
    let mut row_count = 0_usize;
    let table_count = table_snapshots.len();
    for (_, table_snapshot) in table_snapshots {
        row_count = row_count.saturating_add(table_snapshot.row_count);
        change_batches.extend(table_snapshot.change_batches);
    }
    let transaction = snapshot_transaction_batch(source_id, snapshot_lsn, change_batches)?;

    tracing::info!(
        source = %source_id.as_str(),
        tables = table_count,
        max_workers,
        rows = row_count,
        "loaded initial Postgres CDC snapshot with parallel table workers"
    );

    Ok(PostgresSnapshot {
        lsn: snapshot_lsn,
        transaction,
        row_count,
    })
}

fn snapshot_transaction_batch(
    source_id: &CdcSourceId,
    snapshot_lsn: PostgresLsn,
    change_batches: Vec<ChangeBatch>,
) -> Result<Option<TransactionBatch>> {
    if change_batches.is_empty() {
        return Ok(None);
    }
    let position = snapshot_lsn.to_source_position()?;
    Ok(Some(TransactionBatch::new(
        source_id.clone(),
        Some(snapshot_transaction_id(snapshot_lsn)?),
        Some(position.clone()),
        position,
        change_batches,
    )?))
}

fn sorted_snapshot_schemas(schemas: &HashMap<CdcTableId, CdcTableSchema>) -> Vec<&CdcTableSchema> {
    let mut sorted = schemas.values().collect::<Vec<_>>();
    sorted.sort_by(|left, right| {
        left.upstream_table()
            .schema()
            .cmp(right.upstream_table().schema())
            .then(
                left.upstream_table()
                    .table()
                    .cmp(right.upstream_table().table()),
            )
            .then(left.table_id().as_str().cmp(right.table_id().as_str()))
    });
    sorted
}

fn snapshot_lock_sql(schemas: &[&CdcTableSchema]) -> String {
    let mut tables = schemas
        .iter()
        .map(|schema| qualified_table_name(schema.upstream_table()))
        .collect::<Vec<_>>();
    tables.sort();
    tables.dedup();
    format!("LOCK TABLE {} IN SHARE MODE", tables.join(", "))
}

async fn validate_publication_tables(
    transaction: &tokio_postgres::Transaction<'_>,
    publication: &str,
    schemas: &[&CdcTableSchema],
) -> Result<()> {
    let exists_row = transaction
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&publication],
        )
        .await
        .with_context(|| format!("validate Postgres publication '{publication}'"))?;
    let publication_exists: bool = exists_row.get(0);
    ensure!(
        publication_exists,
        "Postgres CDC publication '{publication}' does not exist"
    );

    for schema in schemas {
        let upstream = schema.upstream_table();
        let row = transaction
            .query_one(
                "SELECT EXISTS (
                    SELECT 1
                    FROM pg_publication_tables
                    WHERE pubname = $1
                      AND schemaname = $2
                      AND tablename = $3
                 )",
                &[&publication, &upstream.schema(), &upstream.table()],
            )
            .await
            .with_context(|| {
                format!(
                    "validate Postgres publication '{publication}' includes '{}.{}'",
                    upstream.schema(),
                    upstream.table()
                )
            })?;
        let included: bool = row.get(0);
        ensure!(
            included,
            "Postgres CDC publication '{publication}' does not include table '{}.{}'",
            upstream.schema(),
            upstream.table()
        );
    }

    Ok(())
}

async fn validate_upstream_table_schema(
    transaction: &tokio_postgres::Transaction<'_>,
    schema: &CdcTableSchema,
) -> Result<()> {
    let upstream = schema.upstream_table();
    let column_rows = transaction
        .query(
            "SELECT column_name, is_nullable, data_type, udt_name, numeric_precision, numeric_scale
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position",
            &[&upstream.schema(), &upstream.table()],
        )
        .await
        .with_context(|| {
            format!(
                "discover Postgres table schema for '{}.{}'",
                upstream.schema(),
                upstream.table()
            )
        })?;
    ensure!(
        !column_rows.is_empty(),
        "Postgres CDC table '{}.{}' does not exist or has no columns",
        upstream.schema(),
        upstream.table()
    );

    let mut columns = HashMap::new();
    for row in column_rows {
        let name: String = row.get("column_name");
        let is_nullable: String = row.get("is_nullable");
        let data_type: String = row.get("data_type");
        let udt_name: String = row.get("udt_name");
        let numeric_precision: Option<i32> = row.get("numeric_precision");
        let numeric_scale: Option<i32> = row.get("numeric_scale");
        columns.insert(
            name,
            (
                is_nullable == "YES",
                data_type,
                udt_name,
                numeric_precision,
                numeric_scale,
            ),
        );
    }

    for column in schema.columns() {
        let Some((nullable, data_type, udt_name, numeric_precision, numeric_scale)) =
            columns.get(column.name())
        else {
            bail!(
                "Postgres CDC table '{}.{}' is missing configured column '{}'",
                upstream.schema(),
                upstream.table(),
                column.name()
            );
        };
        ensure!(
            column.nullable() || !nullable,
            "Postgres CDC column '{}.{}' is nullable but Floe table column '{}' is NOT NULL",
            upstream.schema(),
            upstream.table(),
            column.name()
        );
        ensure!(
            postgres_type_compatible(
                column.data_type(),
                udt_name,
                data_type,
                *numeric_precision,
                *numeric_scale
            ),
            "Postgres CDC column '{}.{}' type '{}' is not compatible with Floe type {:?}",
            upstream.schema(),
            upstream.table(),
            udt_name,
            column.data_type()
        );
    }

    let primary_key = discover_primary_key(transaction, upstream).await?;
    ensure!(
        primary_key == schema.primary_key().columns(),
        "Postgres CDC table '{}.{}' primary key {:?} does not match Floe primary key {:?}",
        upstream.schema(),
        upstream.table(),
        primary_key,
        schema.primary_key().columns()
    );

    let replica_identity: String = transaction
        .query_one(
            "SELECT c.relreplident::text
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = $2",
            &[&upstream.schema(), &upstream.table()],
        )
        .await
        .with_context(|| {
            format!(
                "discover Postgres replica identity for '{}.{}'",
                upstream.schema(),
                upstream.table()
            )
        })?
        .get(0);
    ensure!(
        replica_identity != "n",
        "Postgres CDC table '{}.{}' has REPLICA IDENTITY NOTHING",
        upstream.schema(),
        upstream.table()
    );

    Ok(())
}

pub(super) async fn discover_postgres_cdc_table_schema(
    connection_string: &str,
    table_id: CdcTableId,
    upstream: UpstreamTableRef,
) -> Result<CdcTableSchema> {
    let (mut client, connection) =
        tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
            .await
            .context("connect Postgres control plane for CDC schema discovery")?;
    let connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::debug!(error = %err, "Postgres CDC schema discovery connection closed");
        }
    });

    let result = async {
        let transaction = client
            .transaction()
            .await
            .context("begin Postgres CDC schema discovery transaction")?;
        let schema =
            discover_postgres_cdc_table_schema_from_transaction(&transaction, table_id, upstream)
                .await?;
        transaction
            .commit()
            .await
            .context("commit Postgres CDC schema discovery transaction")?;
        Ok(schema)
    }
    .await;
    drop(client);
    connection_task.abort();
    result
}

async fn discover_postgres_cdc_table_schema_from_transaction(
    transaction: &tokio_postgres::Transaction<'_>,
    table_id: CdcTableId,
    upstream: UpstreamTableRef,
) -> Result<CdcTableSchema> {
    let rows = transaction
        .query(
            "SELECT column_name, is_nullable, data_type, udt_name, numeric_precision, numeric_scale
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position",
            &[&upstream.schema(), &upstream.table()],
        )
        .await
        .with_context(|| {
            format!(
                "discover Postgres table schema for '{}.{}'",
                upstream.schema(),
                upstream.table()
            )
        })?;
    ensure!(
        !rows.is_empty(),
        "Postgres CDC table '{}.{}' does not exist or has no columns",
        upstream.schema(),
        upstream.table()
    );

    let mut columns = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.get("column_name");
        let is_nullable: String = row.get("is_nullable");
        let data_type: String = row.get("data_type");
        let udt_name: String = row.get("udt_name");
        let numeric_precision: Option<i32> = row.get("numeric_precision");
        let numeric_scale: Option<i32> = row.get("numeric_scale");
        columns.push(CdcColumn::new(
            name,
            postgres_column_type(&udt_name, &data_type, numeric_precision, numeric_scale)?,
            is_nullable == "YES",
        )?);
    }

    let primary_key = discover_primary_key(transaction, &upstream).await?;
    let replica_identity: String = transaction
        .query_one(
            "SELECT c.relreplident::text
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = $2",
            &[&upstream.schema(), &upstream.table()],
        )
        .await
        .with_context(|| {
            format!(
                "discover Postgres replica identity for '{}.{}'",
                upstream.schema(),
                upstream.table()
            )
        })?
        .get(0);
    ensure!(
        replica_identity != "n",
        "Postgres CDC table '{}.{}' has REPLICA IDENTITY NOTHING",
        upstream.schema(),
        upstream.table()
    );

    CdcTableSchema::new(
        table_id,
        upstream,
        columns,
        CdcPrimaryKey::new(primary_key)?,
    )
}

async fn discover_primary_key(
    transaction: &tokio_postgres::Transaction<'_>,
    upstream: &UpstreamTableRef,
) -> Result<Vec<String>> {
    let rows = transaction
        .query(
            "SELECT a.attname
             FROM pg_index i
             JOIN pg_class c ON c.oid = i.indrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
             JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
             WHERE n.nspname = $1
               AND c.relname = $2
               AND i.indisprimary
             ORDER BY k.ord",
            &[&upstream.schema(), &upstream.table()],
        )
        .await
        .with_context(|| {
            format!(
                "discover Postgres primary key for '{}.{}'",
                upstream.schema(),
                upstream.table()
            )
        })?;
    ensure!(
        !rows.is_empty(),
        "Postgres CDC table '{}.{}' must have a primary key",
        upstream.schema(),
        upstream.table()
    );
    Ok(rows
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect())
}

fn postgres_column_type(
    udt_name: &str,
    data_type: &str,
    numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
) -> Result<ColumnType> {
    let udt_name = udt_name.to_ascii_lowercase();
    let data_type = data_type.to_ascii_lowercase();
    match udt_name.as_str() {
        "int8" | "int4" | "int2" => Ok(ColumnType::Int64),
        "bool" => Ok(ColumnType::Bool),
        "text" | "varchar" | "bpchar" | "name" => Ok(ColumnType::Utf8),
        "timestamp" | "timestamptz" => Ok(ColumnType::TimestampMillis),
        "date" => Ok(ColumnType::DateDays),
        "numeric" => decimal128_type_from_precision_scale(numeric_precision, numeric_scale)
            .unwrap_or(Ok(ColumnType::Numeric)),
        _ if matches!(
            data_type.as_str(),
            "timestamp without time zone" | "timestamp with time zone"
        ) =>
        {
            Ok(ColumnType::TimestampMillis)
        }
        _ => bail!(
            "unsupported Postgres CDC column type '{}' ({}) for schema discovery",
            udt_name,
            data_type
        ),
    }
}

fn postgres_type_compatible(
    expected: &ColumnType,
    udt_name: &str,
    data_type: &str,
    numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
) -> bool {
    let udt_name = udt_name.to_ascii_lowercase();
    let data_type = data_type.to_ascii_lowercase();
    match expected {
        ColumnType::Int64 => matches!(udt_name.as_str(), "int8" | "int4" | "int2"),
        ColumnType::Bool => udt_name == "bool",
        ColumnType::Utf8 => matches!(udt_name.as_str(), "text" | "varchar" | "bpchar" | "name"),
        ColumnType::TimestampMillis => {
            matches!(udt_name.as_str(), "timestamp" | "timestamptz")
                || matches!(
                    data_type.as_str(),
                    "timestamp without time zone" | "timestamp with time zone"
                )
        }
        ColumnType::DateDays => udt_name == "date" || data_type == "date",
        ColumnType::Decimal128 { precision, scale } => {
            (udt_name == "numeric" || matches!(data_type.as_str(), "numeric" | "decimal"))
                && numeric_precision == Some(i32::from(*precision))
                && numeric_scale == Some(i32::from(*scale))
        }
        ColumnType::Numeric => {
            udt_name == "numeric" || matches!(data_type.as_str(), "numeric" | "decimal")
        }
    }
}

fn decimal128_type_from_precision_scale(
    precision: Option<i32>,
    scale: Option<i32>,
) -> Option<Result<ColumnType>> {
    let (Some(precision), Some(scale)) = (precision, scale) else {
        return None;
    };
    if !(1..=38).contains(&precision) || !(0..=precision).contains(&scale) {
        return None;
    }
    Some(ColumnType::decimal128(
        precision as u8,
        i8::try_from(scale).expect("scale <= 38 fits i8"),
    ))
}

struct SnapshotTableChangeBatches {
    change_batches: Vec<ChangeBatch>,
    row_count: usize,
}

async fn snapshot_table_change_batches(
    transaction: &tokio_postgres::Transaction<'_>,
    schema: &CdcTableSchema,
) -> Result<SnapshotTableChangeBatches> {
    let query = snapshot_table_query(schema);
    let started_at = Instant::now();
    let params = std::iter::empty::<&(dyn ToSql + Sync)>();
    let stream = transaction
        .query_raw(&query, params)
        .await
        .with_context(|| {
            format!(
                "snapshot Postgres CDC table '{}.{}'",
                schema.upstream_table().schema(),
                schema.upstream_table().table()
            )
        })?;
    pin_mut!(stream);

    let mut change_batches = Vec::new();
    let mut row_count = 0_usize;
    let rows_per_batch = *POSTGRES_SNAPSHOT_ROWS_PER_BATCH;
    let mut builder = SnapshotColumnarBatchBuilder::new(schema, rows_per_batch);
    while let Some(row) = stream.try_next().await.with_context(|| {
        format!(
            "stream Postgres CDC snapshot table '{}.{}'",
            schema.upstream_table().schema(),
            schema.upstream_table().table()
        )
    })? {
        builder.append_row(schema, &row)?;
        row_count = row_count.saturating_add(1);
        if builder.len() >= rows_per_batch {
            change_batches.push(builder.finish_change_batch(schema)?);
        }
    }
    if !builder.is_empty() {
        change_batches.push(builder.finish_change_batch(schema)?);
    }
    if *CDC_PERF_LOGGING_ENABLED {
        let elapsed = started_at.elapsed();
        tracing::info!(
            table = %schema.table_id().as_str(),
            upstream_schema = %schema.upstream_table().schema(),
            upstream_table = %schema.upstream_table().table(),
            rows = row_count,
            batches = change_batches.len(),
            rows_per_batch,
            elapsed_ms = elapsed.as_millis() as u64,
            rows_per_second = (row_count as f64 / elapsed.as_secs_f64().max(0.001)) as u64,
            "postgres cdc snapshot table streamed"
        );
    }

    Ok(SnapshotTableChangeBatches {
        change_batches,
        row_count,
    })
}

async fn snapshot_table_change_batches_from_exported_snapshot(
    connection_string: &str,
    exported_snapshot: &str,
    schema: &CdcTableSchema,
) -> Result<SnapshotTableChangeBatches> {
    let (mut client, connection) =
        tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
            .await
            .with_context(|| {
                format!(
                    "connect Postgres snapshot worker for '{}.{}'",
                    schema.upstream_table().schema(),
                    schema.upstream_table().table()
                )
            })?;
    let connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::debug!(error = %err, "Postgres snapshot worker connection closed");
        }
    });

    let result = async {
        let transaction = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .context("begin Postgres snapshot worker transaction")?;
        transaction
            .batch_execute(&format!(
                "SET TRANSACTION SNAPSHOT {}",
                quote_pg_literal(exported_snapshot)
            ))
            .await
            .context("bind Postgres snapshot worker to exported snapshot")?;
        let snapshot = snapshot_table_change_batches(&transaction, schema).await;
        transaction
            .commit()
            .await
            .context("commit Postgres snapshot worker transaction")?;
        snapshot
    }
    .await;

    drop(client);
    connection_task.abort();
    result
}

fn snapshot_table_query(schema: &CdcTableSchema) -> String {
    let select_list = schema
        .columns()
        .iter()
        .map(snapshot_select_expr)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT {select_list} FROM {}",
        qualified_table_name(schema.upstream_table())
    )
}

fn snapshot_select_expr(column: &CdcColumn) -> String {
    let quoted = quote_pg_ident(column.name());
    match column.data_type() {
        ColumnType::TimestampMillis => {
            format!("floor(extract(epoch from {quoted}) * 1000)::bigint AS {quoted}")
        }
        ColumnType::DateDays => format!("({quoted} - DATE '1970-01-01')::int AS {quoted}"),
        ColumnType::Decimal128 { .. } => format!("{quoted}::text AS {quoted}"),
        ColumnType::Numeric => format!("{quoted}::text AS {quoted}"),
        ColumnType::Int64 | ColumnType::Bool | ColumnType::Utf8 => quoted,
    }
}

struct SnapshotColumnarBatchBuilder {
    columns: Vec<SnapshotColumnBuilder>,
    len: usize,
    capacity: usize,
}

impl SnapshotColumnarBatchBuilder {
    fn new(schema: &CdcTableSchema, capacity: usize) -> Self {
        Self {
            columns: schema
                .columns()
                .iter()
                .map(|column| SnapshotColumnBuilder::new(column.data_type(), capacity))
                .collect(),
            len: 0,
            capacity,
        }
    }

    fn append_row(&mut self, schema: &CdcTableSchema, row: &tokio_postgres::Row) -> Result<()> {
        ensure!(
            row.columns().len() == schema.columns().len(),
            "Postgres CDC snapshot row has {} columns, expected {}",
            row.columns().len(),
            schema.columns().len()
        );
        for ((builder, column), idx) in self
            .columns
            .iter_mut()
            .zip(schema.columns())
            .zip(0..schema.columns().len())
        {
            builder.append(row, idx, column)?;
        }
        self.len += 1;
        Ok(())
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn finish_change_batch(&mut self, schema: &CdcTableSchema) -> Result<ChangeBatch> {
        let columns = std::mem::take(&mut self.columns)
            .into_iter()
            .map(SnapshotColumnBuilder::finish)
            .collect::<Vec<_>>();
        let rows = CdcColumnarRowBatch::new(columns)?;
        schema.validate_columnar_rows(&rows)?;
        self.columns = schema
            .columns()
            .iter()
            .map(|column| SnapshotColumnBuilder::new(column.data_type(), self.capacity))
            .collect();
        self.len = 0;
        ChangeBatch::new_snapshot_insert(schema.table_id().clone(), rows)
    }
}

enum SnapshotColumnBuilder {
    Int64(Vec<Option<i64>>),
    Bool(Vec<Option<bool>>),
    Utf8(Vec<Option<String>>),
    TimestampMillis(Vec<Option<i64>>),
    DateDays(Vec<Option<i32>>),
    Decimal128 {
        precision: u8,
        scale: i8,
        values: Vec<Option<i128>>,
    },
    Numeric(Vec<Option<String>>),
}

impl SnapshotColumnBuilder {
    fn new(data_type: &ColumnType, capacity: usize) -> Self {
        match data_type {
            ColumnType::Int64 => Self::Int64(Vec::with_capacity(capacity)),
            ColumnType::Bool => Self::Bool(Vec::with_capacity(capacity)),
            ColumnType::Utf8 => Self::Utf8(Vec::with_capacity(capacity)),
            ColumnType::TimestampMillis => Self::TimestampMillis(Vec::with_capacity(capacity)),
            ColumnType::DateDays => Self::DateDays(Vec::with_capacity(capacity)),
            ColumnType::Decimal128 { precision, scale } => Self::Decimal128 {
                precision: *precision,
                scale: *scale,
                values: Vec::with_capacity(capacity),
            },
            ColumnType::Numeric => Self::Numeric(Vec::with_capacity(capacity)),
        }
    }

    fn append(&mut self, row: &tokio_postgres::Row, idx: usize, column: &CdcColumn) -> Result<()> {
        match (self, column.data_type()) {
            (Self::Int64(values), ColumnType::Int64) => {
                values.push(snapshot_int64_value(row, idx, row.columns()[idx].type_())?);
            }
            (Self::Bool(values), ColumnType::Bool) => {
                values.push(row.try_get::<_, Option<bool>>(idx).with_context(|| {
                    format!("decode Postgres CDC snapshot bool '{}'", column.name())
                })?);
            }
            (Self::Utf8(values), ColumnType::Utf8) => {
                values.push(row.try_get::<_, Option<String>>(idx).with_context(|| {
                    format!("decode Postgres CDC snapshot text '{}'", column.name())
                })?);
            }
            (Self::TimestampMillis(values), ColumnType::TimestampMillis) => {
                values.push(row.try_get::<_, Option<i64>>(idx).with_context(|| {
                    format!(
                        "decode Postgres CDC snapshot timestamp millis '{}'",
                        column.name()
                    )
                })?);
            }
            (Self::DateDays(values), ColumnType::DateDays) => {
                values.push(row.try_get::<_, Option<i32>>(idx).with_context(|| {
                    format!("decode Postgres CDC snapshot date days '{}'", column.name())
                })?);
            }
            (Self::Decimal128 { scale, values, .. }, ColumnType::Decimal128 { .. }) => {
                let value = row.try_get::<_, Option<String>>(idx).with_context(|| {
                    format!(
                        "decode Postgres CDC snapshot decimal128 '{}'",
                        column.name()
                    )
                })?;
                values.push(
                    value
                        .as_deref()
                        .map(|value| parse_decimal_text_to_i128(value, *scale))
                        .transpose()?,
                );
            }
            (Self::Numeric(values), ColumnType::Numeric) => {
                values.push(row.try_get::<_, Option<String>>(idx).with_context(|| {
                    format!("decode Postgres CDC snapshot numeric '{}'", column.name())
                })?);
            }
            _ => bail!(
                "Postgres CDC snapshot builder for column '{}' does not match type {:?}",
                column.name(),
                column.data_type()
            ),
        }
        Ok(())
    }

    fn finish(self) -> CdcColumnarColumn {
        match self {
            Self::Int64(values) => CdcColumnarColumn::Int64(values),
            Self::Bool(values) => CdcColumnarColumn::Bool(values),
            Self::Utf8(values) => CdcColumnarColumn::Utf8(values),
            Self::TimestampMillis(values) => CdcColumnarColumn::TimestampMillis(values),
            Self::DateDays(values) => CdcColumnarColumn::DateDays(values),
            Self::Decimal128 {
                precision,
                scale,
                values,
            } => CdcColumnarColumn::Decimal128 {
                precision,
                scale,
                values,
            },
            Self::Numeric(values) => CdcColumnarColumn::Numeric(values),
        }
    }
}

fn parse_decimal_text_to_i128(value: &str, scale: i8) -> Result<i128> {
    let scale = u32::try_from(scale).context("Decimal128 scale cannot be negative")?;
    let value = value.trim();
    ensure!(!value.is_empty(), "decimal value cannot be empty");

    let (negative, digits) = value
        .strip_prefix('-')
        .map(|rest| (true, rest))
        .unwrap_or_else(|| {
            value
                .strip_prefix('+')
                .map(|rest| (false, rest))
                .unwrap_or((false, value))
        });

    let mut parsed = 0_i128;
    let mut saw_digit = false;
    let mut saw_decimal = false;
    let mut fraction_len = 0_usize;
    let scale_usize = usize::try_from(scale).expect("u32 scale fits usize");

    for byte in digits.bytes() {
        match byte {
            b'0'..=b'9' => {
                saw_digit = true;
                if saw_decimal {
                    fraction_len = fraction_len.saturating_add(1);
                    ensure!(
                        fraction_len <= scale_usize,
                        "decimal value '{value}' has more fractional digits than scale {scale}"
                    );
                }
                parsed = parsed
                    .checked_mul(10)
                    .and_then(|acc| acc.checked_add(i128::from(byte - b'0')))
                    .with_context(|| format!("decimal value '{value}' exceeds i128 range"))?;
            }
            b'.' if !saw_decimal => {
                saw_decimal = true;
            }
            _ => bail!("invalid decimal value '{value}'"),
        }
    }

    ensure!(saw_digit, "decimal value '{value}' has no digits");
    for _ in 0..scale_usize.saturating_sub(fraction_len) {
        parsed = parsed
            .checked_mul(10)
            .with_context(|| format!("decimal value '{value}' exceeds i128 range"))?;
    }

    Ok(if negative { -parsed } else { parsed })
}

fn snapshot_int64_value(
    row: &tokio_postgres::Row,
    idx: usize,
    postgres_type: &Type,
) -> Result<Option<i64>> {
    match *postgres_type {
        Type::INT8 => row
            .try_get::<_, Option<i64>>(idx)
            .context("decode Postgres CDC snapshot int8"),
        Type::INT4 => row
            .try_get::<_, Option<i32>>(idx)
            .context("decode Postgres CDC snapshot int4")
            .map(|value| value.map(i64::from)),
        Type::INT2 => row
            .try_get::<_, Option<i16>>(idx)
            .context("decode Postgres CDC snapshot int2")
            .map(|value| value.map(i64::from)),
        _ => bail!("unsupported Postgres integer snapshot type {postgres_type}"),
    }
}

fn snapshot_checkpoint(source_id: &CdcSourceId, lsn: PostgresLsn) -> Result<CdcCheckpoint> {
    Ok(CdcCheckpoint::new(
        source_id.clone(),
        lsn.to_source_position()?,
        Some(snapshot_transaction_id(lsn)?),
    ))
}

fn snapshot_transaction_id(lsn: PostgresLsn) -> Result<CdcTransactionId> {
    CdcTransactionId::new(format!("snapshot:{}", lsn.to_pg_string()))
}

async fn wait_for_postgres_snapshot_commit(
    receiver: Option<&mut watch::Receiver<PostgresCdcCommit>>,
    slot: &str,
    target_lsn: PostgresLsn,
    cancel: &CancellationToken,
) -> Result<()> {
    let Some(receiver) = receiver else {
        bail!("cannot wait for initial Postgres snapshot durability without commit receiver");
    };

    loop {
        let commit = receiver.borrow_and_update().clone();
        if postgres_commit_covers_lsn(&commit, slot, target_lsn)? {
            return Ok(());
        }

        tokio::select! {
            _ = cancel.cancelled() => {
                bail!("cancelled while waiting for initial Postgres snapshot durability");
            }
            changed = receiver.changed() => {
                changed.context("Postgres CDC commit channel closed before initial snapshot became durable")?;
            }
        }
    }
}

fn postgres_commit_covers_lsn(
    commit: &PostgresCdcCommit,
    slot: &str,
    target_lsn: PostgresLsn,
) -> Result<bool> {
    let Some(slot_commit) = commit.slots.iter().find(|entry| entry.slot == slot) else {
        return Ok(false);
    };
    Ok(PostgresLsn::parse(&slot_commit.lsn)?.as_u64() >= target_lsn.as_u64())
}

fn qualified_table_name(upstream: &UpstreamTableRef) -> String {
    format!(
        "{}.{}",
        quote_pg_ident(upstream.schema()),
        quote_pg_ident(upstream.table())
    )
}

fn quote_pg_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn quote_pg_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use floe_core::RowValue;
    use std::sync::Arc;

    #[test]
    fn parses_decimal_text_without_allocation_sensitive_edge_cases() {
        assert_eq!(parse_decimal_text_to_i128("123.45", 2).unwrap(), 12_345);
        assert_eq!(parse_decimal_text_to_i128("123", 2).unwrap(), 12_300);
        assert_eq!(parse_decimal_text_to_i128("-0.07", 2).unwrap(), -7);
        assert_eq!(parse_decimal_text_to_i128("+42.1", 3).unwrap(), 42_100);
        assert_eq!(parse_decimal_text_to_i128(" .5 ", 2).unwrap(), 50);
    }

    #[test]
    fn quotes_exported_snapshot_literal() {
        assert_eq!(
            quote_pg_literal("00000003-0000001B-1"),
            "'00000003-0000001B-1'"
        );
        assert_eq!(quote_pg_literal("snap'shot"), "'snap''shot'");
    }

    #[test]
    fn rejects_decimal_text_that_cannot_match_scale() {
        assert!(parse_decimal_text_to_i128("1.234", 2).is_err());
        assert!(parse_decimal_text_to_i128("1.2.3", 2).is_err());
        assert!(parse_decimal_text_to_i128("", 2).is_err());
        assert!(parse_decimal_text_to_i128("abc", 2).is_err());
        assert!(parse_decimal_text_to_i128("1.0", -1).is_err());
    }

    #[tokio::test]
    async fn cancelled_snapshot_before_commit_leaves_no_checkpoint_for_retry() {
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let table_id = CdcTableId::new("orders").expect("table id");
        let catalog = floe_storage::SlateCatalog::in_memory()
            .await
            .expect("catalog");
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(catalog.db()));
        let table_store = CdcTableStore::new(table);
        let runtime_plan = PostgresCdcRuntimePlan {
            source_id: source_id.clone(),
            schemas: HashMap::new(),
            materialized_table_ids: HashSet::new(),
            replication_pipelines: Vec::new(),
        };
        let lsn = PostgresLsn::from_u64(120);
        let snapshot = PostgresSnapshot {
            lsn,
            transaction: snapshot_transaction_batch(
                &source_id,
                lsn,
                vec![
                    ChangeBatch::new(
                        table_id,
                        vec![CdcChange::Insert {
                            row: floe_cdc_core::CdcRow::new([
                                Some(RowValue::Int64(1)),
                                Some(RowValue::Utf8("snapshot".to_string())),
                            ])
                            .expect("row"),
                        }],
                    )
                    .expect("snapshot change batch"),
                ],
            )
            .expect("snapshot transaction"),
            row_count: 1,
        };
        let (sender, mut receiver) = mpsc::channel(1);
        let (_commit_sender, mut commit_receiver) = watch::channel(PostgresCdcCommit::default());
        let cancel = CancellationToken::new();
        cancel.cancel();

        let err = finish_loaded_postgres_snapshot(
            "slot",
            "publication",
            &runtime_plan,
            &table_store,
            &sender,
            Some(&mut commit_receiver),
            &cancel,
            snapshot,
        )
        .await
        .expect_err("cancelled snapshot should not finish");

        assert!(
            format!("{err:#}").contains("cancelled while waiting for initial Postgres snapshot")
        );
        let queued = receiver.recv().await.expect("queued snapshot transaction");
        assert_eq!(queued.slot, "slot");
        assert_eq!(queued.source_id, source_id);
        assert_eq!(
            queued
                .transaction
                .transaction_id()
                .map(CdcTransactionId::as_str),
            Some("snapshot:0/78")
        );
        assert_eq!(
            table_store
                .load_checkpoint(&queued.source_id)
                .await
                .expect("load checkpoint"),
            None
        );
    }
}
