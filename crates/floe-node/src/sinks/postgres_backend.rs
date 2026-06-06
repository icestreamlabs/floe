use std::collections::HashSet;

use anyhow::ensure;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::task::JoinHandle;

use super::*;

const POSTGRES_COPY_CHUNK_BYTES: usize = 1024 * 1024;
const POSTGRES_INSERT_STAGE_TABLE: &str = "floe_sink_stage_rows";
const POSTGRES_DELETE_STAGE_TABLE: &str = "floe_sink_stage_deletes";
const POSTGRES_STAGE_ROW_INDEX_COLUMN: &str = "__floe_row_idx";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PostgresSinkMode {
    Upsert,
    AppendOnly,
}

impl PostgresSinkMode {
    fn parse(value: Option<&str>) -> Result<Self> {
        match value
            .unwrap_or("upsert")
            .to_ascii_lowercase()
            .replace('-', "_")
            .as_str()
        {
            "upsert" => Ok(Self::Upsert),
            "append_only" => Ok(Self::AppendOnly),
            other => bail!("unsupported Postgres sink mode '{other}'"),
        }
    }
}

pub(super) struct PostgresSinkConfig<'a> {
    pub(super) sink_name: &'a str,
    pub(super) changelog: ChangelogSourceConfig<'a>,
    pub(super) connection: &'a str,
    pub(super) table: &'a str,
    pub(super) mode: Option<&'a str>,
    pub(super) primary_key: Vec<String>,
    pub(super) retry_policy: RetryPolicy,
    pub(super) checkpoint_tx: Option<SinkCheckpointSender>,
}

pub(super) async fn run_postgres_sink(config: PostgresSinkConfig<'_>) -> Result<()> {
    let mode = PostgresSinkMode::parse(config.mode)?;
    let mut stream = execute_mv_changelog(
        config.changelog.registry.as_ref(),
        config.changelog.params(),
        config.changelog.cancel.clone(),
    )
    .await?;
    let schema = PostgresSinkSchema::from_arrow(stream.schema(), config.primary_key, mode)?;
    let mut writer = PostgresSinkWriter::new(config.connection, config.table, mode, schema)?;

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        apply_postgres_batch_with_retry(
            &mut writer,
            &batch,
            config.retry_policy,
            &config.changelog.cancel,
        )
        .await
        .with_context(|| {
            format!(
                "apply MV changelog version {} to Postgres sink '{sink_name}'",
                batch.version,
                sink_name = config.sink_name
            )
        })?;
        publish_sink_cursor(
            &config.checkpoint_tx,
            SinkCursor {
                sink: config.sink_name.to_string(),
                mv_name: config.changelog.mv.to_string(),
                last_emitted_mv_version: batch.version,
                row_index: None,
            },
        )
        .await?;
    }

    Ok(())
}

async fn apply_postgres_batch_with_retry(
    writer: &mut PostgresSinkWriter,
    batch: &MvChangelogBatch,
    retry_policy: RetryPolicy,
    cancel: &CancellationToken,
) -> Result<()> {
    for attempt in 0..retry_policy.max_attempts {
        match writer.apply_batch(batch).await {
            Ok(()) => return Ok(()),
            Err(err) if attempt + 1 >= retry_policy.max_attempts => return Err(err),
            Err(err) => {
                writer.disconnect();
                let backoff = retry_policy.backoff_for_failure(attempt);
                tracing::warn!(
                    attempt = attempt + 1,
                    max_attempts = retry_policy.max_attempts,
                    retry_delay_ms = backoff.as_millis() as u64,
                    error = %err,
                    "Postgres sink batch apply failed; retrying"
                );
                wait_for_sink_retry_backoff(backoff, cancel).await?;
            }
        }
    }
    bail!("Postgres sink batch apply failed without an error")
}

struct PostgresSinkWriter {
    connection_string: String,
    target_table: String,
    mode: PostgresSinkMode,
    schema: PostgresSinkSchema,
    insert_sql: String,
    delete_sql: Option<String>,
    connection: Option<PostgresSinkConnection>,
    insert_stage_ready: bool,
    delete_stage_ready: bool,
}

struct PostgresSinkConnection {
    client: tokio_postgres::Client,
    task: JoinHandle<()>,
}

impl Drop for PostgresSinkConnection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl PostgresSinkWriter {
    fn new(
        connection: &str,
        table: &str,
        mode: PostgresSinkMode,
        schema: PostgresSinkSchema,
    ) -> Result<Self> {
        ensure!(
            !connection.trim().is_empty(),
            "Postgres sink connection cannot be empty"
        );
        let target_table = quote_postgres_qualified_name(table)?;
        let insert_sql = match mode {
            PostgresSinkMode::Upsert => {
                postgres_upsert_from_stage_sql(&schema, table, POSTGRES_INSERT_STAGE_TABLE)?
            }
            PostgresSinkMode::AppendOnly => {
                postgres_append_insert_from_stage_sql(&schema, table, POSTGRES_INSERT_STAGE_TABLE)?
            }
        };
        let delete_sql = match mode {
            PostgresSinkMode::Upsert => Some(postgres_delete_using_stage_sql(
                &schema,
                table,
                POSTGRES_DELETE_STAGE_TABLE,
            )?),
            PostgresSinkMode::AppendOnly => None,
        };
        Ok(Self {
            connection_string: connection.to_string(),
            target_table,
            mode,
            schema,
            insert_sql,
            delete_sql,
            connection: None,
            insert_stage_ready: false,
            delete_stage_ready: false,
        })
    }

    async fn apply_batch(&mut self, batch: &MvChangelogBatch) -> Result<()> {
        if batch.batch.num_rows() == 0 {
            return Ok(());
        }
        let actions = self.batch_actions(batch)?;
        if actions.is_empty() {
            return Ok(());
        }
        let target_table = self.target_table.clone();
        let insert_sql = self.insert_sql.clone();
        let delete_sql = self.delete_sql.clone();
        let mode = self.mode;
        let mut insert_stage_ready = self.insert_stage_ready;
        let mut delete_stage_ready = self.delete_stage_ready;
        self.ensure_connected().await?;
        let connection = self
            .connection
            .as_mut()
            .context("Postgres sink connection is not initialized")?;
        let transaction = connection
            .client
            .transaction()
            .await
            .with_context(|| format!("start Postgres sink transaction for {target_table}"))?;

        if !actions.deletes.is_empty() {
            let delete_sql = delete_sql
                .as_deref()
                .context("Postgres upsert sink is missing delete statement")?;
            let key_columns = self
                .schema
                .key_column_indexes
                .iter()
                .map(|idx| &self.schema.columns[*idx])
                .collect::<Vec<_>>();
            prepare_stage_table(
                &transaction,
                POSTGRES_DELETE_STAGE_TABLE,
                key_columns.len(),
                false,
                &mut delete_stage_ready,
            )
            .await
            .with_context(|| {
                format!("prepare Postgres sink delete stage table for {target_table}")
            })?;
            copy_stage_rows(
                &transaction,
                POSTGRES_DELETE_STAGE_TABLE,
                key_columns.len(),
                &actions.deletes,
                false,
            )
            .await
            .with_context(|| format!("COPY delete keys into Postgres sink stage {target_table}"))?;
            transaction
                .execute(delete_sql, &[])
                .await
                .with_context(|| {
                    format!("bulk delete MV rows from Postgres sink {target_table}")
                })?;
        }

        if !actions.inserts.is_empty() {
            prepare_stage_table(
                &transaction,
                POSTGRES_INSERT_STAGE_TABLE,
                self.schema.columns.len(),
                true,
                &mut insert_stage_ready,
            )
            .await
            .with_context(|| {
                format!("prepare Postgres sink insert stage table for {target_table}")
            })?;
            copy_stage_rows(
                &transaction,
                POSTGRES_INSERT_STAGE_TABLE,
                self.schema.columns.len(),
                &actions.inserts,
                true,
            )
            .await
            .with_context(|| format!("COPY MV rows into Postgres sink stage {target_table}"))?;
            transaction
                .execute(&insert_sql, &[])
                .await
                .with_context(|| match mode {
                    PostgresSinkMode::Upsert => {
                        format!("bulk upsert MV rows into Postgres sink {target_table}")
                    }
                    PostgresSinkMode::AppendOnly => {
                        format!("bulk insert MV rows into append-only Postgres sink {target_table}")
                    }
                })?;
        }

        transaction
            .commit()
            .await
            .with_context(|| format!("commit Postgres sink transaction for {target_table}"))?;
        self.insert_stage_ready = insert_stage_ready;
        self.delete_stage_ready = delete_stage_ready;
        Ok(())
    }

    fn batch_actions(&self, batch: &MvChangelogBatch) -> Result<PostgresBatchActions> {
        let mut deletes = Vec::new();
        let mut inserts = Vec::new();
        if self.mode == PostgresSinkMode::Upsert {
            for row_idx in 0..batch.batch.num_rows() {
                let diff = batch.diffs.get(row_idx).copied().unwrap_or(1);
                if diff < 0 {
                    deletes.push(self.key_params(batch, row_idx)?);
                }
            }
        }

        for row_idx in 0..batch.batch.num_rows() {
            let diff = batch.diffs.get(row_idx).copied().unwrap_or(1);
            match self.mode {
                PostgresSinkMode::Upsert if diff > 0 => {
                    inserts.push(self.row_params(batch, row_idx)?);
                }
                PostgresSinkMode::Upsert => {}
                PostgresSinkMode::AppendOnly if diff > 0 => {
                    let repeats = usize::try_from(diff)
                        .context("Postgres append-only sink diff exceeds usize")?;
                    for _ in 0..repeats {
                        inserts.push(self.row_params(batch, row_idx)?);
                    }
                }
                PostgresSinkMode::AppendOnly if diff == 0 => {}
                PostgresSinkMode::AppendOnly => {
                    bail!(
                        "Postgres append-only sink received negative diff {diff} at MV version {} row {row_idx}",
                        batch.version
                    );
                }
            }
        }
        Ok(PostgresBatchActions { deletes, inserts })
    }

    async fn ensure_connected(&mut self) -> Result<()> {
        if self.connection.is_some() {
            return Ok(());
        }
        let (client, connection) =
            tokio_postgres::connect(self.connection_string.as_str(), tokio_postgres::NoTls)
                .await
                .with_context(|| format!("connect Postgres sink target {}", self.target_table))?;
        let target_table = self.target_table.clone();
        let task = tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(
                    table = %target_table,
                    error = %err,
                    "Postgres sink connection task failed"
                );
            }
        });
        self.connection = Some(PostgresSinkConnection { client, task });
        self.insert_stage_ready = false;
        self.delete_stage_ready = false;
        Ok(())
    }

    fn disconnect(&mut self) {
        self.connection = None;
        self.insert_stage_ready = false;
        self.delete_stage_ready = false;
    }

    fn row_params(
        &self,
        batch: &MvChangelogBatch,
        row_idx: usize,
    ) -> Result<Vec<PostgresParamValue>> {
        self.schema
            .columns
            .iter()
            .enumerate()
            .map(|(column_idx, column)| {
                let value = arrow_value_to_row_value(
                    batch.batch.column(column_idx).as_ref(),
                    row_idx,
                    &column.data_type,
                )?;
                postgres_param_from_row_value(column, value)
            })
            .collect()
    }

    fn key_params(
        &self,
        batch: &MvChangelogBatch,
        row_idx: usize,
    ) -> Result<Vec<PostgresParamValue>> {
        self.schema
            .key_column_indexes
            .iter()
            .map(|column_idx| {
                let column = &self.schema.columns[*column_idx];
                let value = arrow_value_to_row_value(
                    batch.batch.column(*column_idx).as_ref(),
                    row_idx,
                    &column.data_type,
                )?;
                postgres_param_from_row_value(column, value)
            })
            .collect()
    }
}

#[derive(Debug)]
struct PostgresBatchActions {
    deletes: Vec<Vec<PostgresParamValue>>,
    inserts: Vec<Vec<PostgresParamValue>>,
}

impl PostgresBatchActions {
    fn is_empty(&self) -> bool {
        self.deletes.is_empty() && self.inserts.is_empty()
    }
}

struct PostgresSinkSchema {
    columns: Vec<PostgresSinkColumn>,
    key_columns: Vec<String>,
    key_column_indexes: Vec<usize>,
}

struct PostgresSinkColumn {
    name: String,
    data_type: ColumnType,
    nullable: bool,
    key: bool,
}

impl PostgresSinkSchema {
    fn from_arrow(
        schema: SchemaRef,
        primary_key: Vec<String>,
        mode: PostgresSinkMode,
    ) -> Result<Self> {
        if mode == PostgresSinkMode::Upsert && primary_key.is_empty() {
            bail!("Postgres upsert sink requires primary_key");
        }
        let key_names = primary_key
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let mut seen_fields = HashSet::new();
        let mut columns = Vec::with_capacity(schema.fields().len());
        let mut key_column_indexes = Vec::new();
        for (column_idx, field) in schema.fields().iter().enumerate() {
            ensure!(
                seen_fields.insert(field.name().as_str()),
                "Postgres sink MV schema contains duplicate column '{}'",
                field.name()
            );
            let key = key_names.contains(field.name().as_str());
            if key {
                key_column_indexes.push(column_idx);
            }
            columns.push(PostgresSinkColumn {
                name: field.name().clone(),
                data_type: column_type_from_arrow(field.data_type())?,
                nullable: field.is_nullable(),
                key,
            });
        }
        for key_column in &primary_key {
            ensure!(
                columns.iter().any(|column| column.name == *key_column),
                "Postgres sink primary_key column '{key_column}' is not present in MV schema"
            );
        }
        Ok(Self {
            columns,
            key_columns: primary_key,
            key_column_indexes,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
enum PostgresParamValue {
    Int64(Option<i64>),
    Bool(Option<bool>),
    Text(Option<String>),
    TimestampMillis(Option<i64>),
    Int32(Option<i32>),
}

impl PostgresParamValue {
    fn null(data_type: &ColumnType) -> Self {
        match data_type {
            ColumnType::Int64 => Self::Int64(None),
            ColumnType::Bool => Self::Bool(None),
            ColumnType::Utf8 => Self::Text(None),
            ColumnType::TimestampMillis => Self::TimestampMillis(None),
            ColumnType::DateDays => Self::Int32(None),
            ColumnType::Decimal128 { .. } | ColumnType::Numeric => Self::Text(None),
        }
    }

    fn copy_text(&self) -> Option<String> {
        match self {
            Self::Int64(value) => value.map(|value| value.to_string()),
            Self::Bool(value) => value.map(|value| value.to_string()),
            Self::Text(value) => value.clone(),
            Self::TimestampMillis(value) => value.map(|value| value.to_string()),
            Self::Int32(value) => value.map(|value| value.to_string()),
        }
    }
}

fn postgres_param_from_row_value(
    column: &PostgresSinkColumn,
    value: Option<RowValue>,
) -> Result<PostgresParamValue> {
    let Some(value) = value else {
        ensure!(
            column.nullable && !column.key,
            "Postgres sink column '{}' cannot be NULL",
            column.name
        );
        return Ok(PostgresParamValue::null(&column.data_type));
    };
    match (&column.data_type, value) {
        (ColumnType::Int64, RowValue::Int64(value)) => Ok(PostgresParamValue::Int64(Some(value))),
        (ColumnType::Bool, RowValue::Bool(value)) => Ok(PostgresParamValue::Bool(Some(value))),
        (ColumnType::Utf8, RowValue::Utf8(value)) => Ok(PostgresParamValue::Text(Some(value))),
        (ColumnType::TimestampMillis, RowValue::TimestampMillis(value)) => {
            Ok(PostgresParamValue::TimestampMillis(Some(value)))
        }
        (ColumnType::DateDays, RowValue::DateDays(value)) => {
            Ok(PostgresParamValue::Int32(Some(value)))
        }
        (ColumnType::Decimal128 { scale, .. }, RowValue::Decimal128(value)) => Ok(
            PostgresParamValue::Text(Some(format_decimal128(value, *scale))),
        ),
        (ColumnType::Numeric, RowValue::Numeric(value)) => {
            Ok(PostgresParamValue::Text(Some(value)))
        }
        (expected, actual) => bail!(
            "Postgres sink column '{}' expected {expected:?} but received {actual:?}",
            column.name
        ),
    }
}

async fn prepare_stage_table(
    transaction: &tokio_postgres::Transaction<'_>,
    stage_table: &str,
    column_count: usize,
    include_row_index: bool,
    stage_ready: &mut bool,
) -> Result<()> {
    if *stage_ready {
        let sql = format!("TRUNCATE {}", quote_postgres_ident(stage_table));
        transaction.batch_execute(&sql).await?;
        return Ok(());
    }

    let mut columns = (0..column_count)
        .map(|idx| format!("{} text", quote_postgres_ident(&stage_column_name(idx))))
        .collect::<Vec<_>>();
    if include_row_index {
        columns.push(format!(
            "{} bigint",
            quote_postgres_ident(POSTGRES_STAGE_ROW_INDEX_COLUMN)
        ));
    }
    let sql = format!(
        "CREATE TEMP TABLE {} ({}) ON COMMIT PRESERVE ROWS",
        quote_postgres_ident(stage_table),
        columns.join(", ")
    );
    transaction.batch_execute(&sql).await?;
    *stage_ready = true;
    Ok(())
}

async fn copy_stage_rows(
    transaction: &tokio_postgres::Transaction<'_>,
    stage_table: &str,
    column_count: usize,
    rows: &[Vec<PostgresParamValue>],
    include_row_index: bool,
) -> Result<u64> {
    let copy_sql = postgres_copy_sql(stage_table, column_count, include_row_index);
    let mut sink = Box::pin(transaction.copy_in::<_, Bytes>(&copy_sql).await?);
    let mut buffer = String::with_capacity(POSTGRES_COPY_CHUNK_BYTES.min(64 * 1024));
    for (row_idx, row) in rows.iter().enumerate() {
        ensure!(
            row.len() == column_count,
            "Postgres sink stage row has {} values, expected {column_count}",
            row.len()
        );
        append_copy_row(&mut buffer, row, include_row_index.then_some(row_idx));
        if buffer.len() >= POSTGRES_COPY_CHUNK_BYTES {
            sink.send(Bytes::from(std::mem::take(&mut buffer))).await?;
        }
    }
    if !buffer.is_empty() {
        sink.send(Bytes::from(buffer)).await?;
    }
    let copied = sink.as_mut().finish().await?;
    Ok(copied)
}

fn append_copy_row(out: &mut String, row: &[PostgresParamValue], row_idx: Option<usize>) {
    for (idx, value) in row.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        append_copy_field(out, value.copy_text().as_deref());
    }
    if let Some(row_idx) = row_idx {
        if !row.is_empty() {
            out.push(',');
        }
        out.push_str(&row_idx.to_string());
    }
    out.push('\n');
}

fn append_copy_field(out: &mut String, value: Option<&str>) {
    let Some(value) = value else {
        out.push_str("\\N");
        return;
    };
    out.push('"');
    for ch in value.chars() {
        if ch == '"' {
            out.push('"');
        }
        out.push(ch);
    }
    out.push('"');
}

fn postgres_copy_sql(stage_table: &str, column_count: usize, include_row_index: bool) -> String {
    let mut columns = (0..column_count)
        .map(|idx| quote_postgres_ident(&stage_column_name(idx)))
        .collect::<Vec<_>>();
    if include_row_index {
        columns.push(quote_postgres_ident(POSTGRES_STAGE_ROW_INDEX_COLUMN));
    }
    format!(
        "COPY {} ({}) FROM STDIN WITH (FORMAT csv, NULL '\\N')",
        quote_postgres_ident(stage_table),
        columns.join(", ")
    )
}

fn postgres_append_insert_from_stage_sql(
    schema: &PostgresSinkSchema,
    table: &str,
    stage_table: &str,
) -> Result<String> {
    let table = quote_postgres_qualified_name(table)?;
    let columns = schema
        .columns
        .iter()
        .map(|column| quote_postgres_ident(&column.name))
        .collect::<Vec<_>>();
    let values = schema
        .columns
        .iter()
        .enumerate()
        .map(|(idx, column)| postgres_stage_value_expr("s", idx, &column.data_type))
        .collect::<Vec<_>>();
    Ok(format!(
        "INSERT INTO {table} ({}) SELECT {} FROM {} AS s",
        columns.join(", "),
        values.join(", "),
        quote_postgres_ident(stage_table)
    ))
}

fn postgres_upsert_from_stage_sql(
    schema: &PostgresSinkSchema,
    table: &str,
    stage_table: &str,
) -> Result<String> {
    let table = quote_postgres_qualified_name(table)?;
    let columns = schema
        .columns
        .iter()
        .map(|column| quote_postgres_ident(&column.name))
        .collect::<Vec<_>>();
    let values = schema
        .columns
        .iter()
        .enumerate()
        .map(|(idx, column)| postgres_stage_value_expr("s", idx, &column.data_type))
        .collect::<Vec<_>>();
    let primary_keys = schema
        .key_columns
        .iter()
        .map(|column| quote_postgres_ident(column))
        .collect::<Vec<_>>();
    let primary_key_names = schema
        .key_columns
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let updates = schema
        .columns
        .iter()
        .filter(|column| !primary_key_names.contains(column.name.as_str()))
        .map(|column| {
            let quoted = quote_postgres_ident(&column.name);
            format!("{quoted} = EXCLUDED.{quoted}")
        })
        .collect::<Vec<_>>();
    let conflict_action = if updates.is_empty() {
        "DO NOTHING".to_string()
    } else {
        format!("DO UPDATE SET {}", updates.join(", "))
    };
    let source_relation = postgres_upsert_stage_relation_sql(schema, stage_table);
    Ok(format!(
        "INSERT INTO {table} ({}) SELECT {} FROM {source_relation} AS s ON CONFLICT ({}) {conflict_action}",
        columns.join(", "),
        values.join(", "),
        primary_keys.join(", ")
    ))
}

fn postgres_upsert_stage_relation_sql(schema: &PostgresSinkSchema, stage_table: &str) -> String {
    let key_refs = schema
        .key_column_indexes
        .iter()
        .map(|idx| quote_postgres_ident(&stage_column_name(*idx)))
        .collect::<Vec<_>>();
    format!(
        "(SELECT DISTINCT ON ({}) * FROM {} ORDER BY {}, {} DESC)",
        key_refs.join(", "),
        quote_postgres_ident(stage_table),
        key_refs.join(", "),
        quote_postgres_ident(POSTGRES_STAGE_ROW_INDEX_COLUMN)
    )
}

fn postgres_delete_using_stage_sql(
    schema: &PostgresSinkSchema,
    table: &str,
    stage_table: &str,
) -> Result<String> {
    let table = quote_postgres_qualified_name(table)?;
    let predicates = schema
        .key_columns
        .iter()
        .enumerate()
        .map(|(idx, column_name)| {
            let column_idx = schema
                .key_column_indexes
                .iter()
                .find(|column_idx| schema.columns[**column_idx].name == *column_name)
                .copied()
                .ok_or_else(|| {
                    anyhow!("Postgres sink primary-key column '{column_name}' missing from schema")
                })?;
            let column = &schema.columns[column_idx];
            Ok(format!(
                "{} = {}",
                postgres_target_column_ref("t", &column.name),
                postgres_stage_value_expr("s", idx, &column.data_type)
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(format!(
        "DELETE FROM {table} AS t USING {} AS s WHERE {}",
        quote_postgres_ident(stage_table),
        predicates.join(" AND ")
    ))
}

fn postgres_stage_value_expr(alias: &str, column_idx: usize, data_type: &ColumnType) -> String {
    let column = postgres_stage_column_ref(alias, column_idx);
    match data_type {
        ColumnType::TimestampMillis => format!("to_timestamp({column}::double precision / 1000.0)"),
        ColumnType::DateDays => format!("DATE '1970-01-01' + {column}::integer"),
        ColumnType::Decimal128 { .. } | ColumnType::Numeric => {
            format!("{column}::numeric")
        }
        ColumnType::Int64 => format!("{column}::bigint"),
        ColumnType::Bool => format!("{column}::boolean"),
        ColumnType::Utf8 => column,
    }
}

fn postgres_stage_column_ref(alias: &str, column_idx: usize) -> String {
    format!(
        "{}.{}",
        quote_postgres_ident(alias),
        quote_postgres_ident(&stage_column_name(column_idx))
    )
}

fn postgres_target_column_ref(alias: &str, column_name: &str) -> String {
    format!(
        "{}.{}",
        quote_postgres_ident(alias),
        quote_postgres_ident(column_name)
    )
}

fn stage_column_name(idx: usize) -> String {
    format!("c{idx}")
}

fn quote_postgres_qualified_name(name: &str) -> Result<String> {
    let parts = name
        .split('.')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(quote_postgres_ident)
        .collect::<Vec<_>>();
    ensure!(!parts.is_empty(), "Postgres sink table cannot be empty");
    Ok(parts.join("."))
}

fn quote_postgres_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Date32Array, Decimal128Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use floe_executor::mv_changelog::MvChangelogBatchKind;

    #[test]
    fn postgres_sink_writer_builds_sql_and_orders_upsert_actions() {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, true),
            Field::new("order_date", DataType::Date32, true),
            Field::new("amount", DataType::Decimal128(12, 2), true),
        ]));
        let schema = PostgresSinkSchema::from_arrow(
            Arc::clone(&arrow_schema),
            vec!["id".to_string()],
            PostgresSinkMode::Upsert,
        )
        .expect("schema");
        let writer = PostgresSinkWriter::new(
            "postgres://postgres:postgres@localhost/postgres",
            "public.orders_copy",
            PostgresSinkMode::Upsert,
            schema,
        )
        .expect("writer");

        assert_eq!(
            writer.insert_sql,
            "INSERT INTO \"public\".\"orders_copy\" (\"id\", \"status\", \"order_date\", \"amount\") SELECT \"s\".\"c0\"::bigint, \"s\".\"c1\", DATE '1970-01-01' + \"s\".\"c2\"::integer, \"s\".\"c3\"::numeric FROM (SELECT DISTINCT ON (\"c0\") * FROM \"floe_sink_stage_rows\" ORDER BY \"c0\", \"__floe_row_idx\" DESC) AS s ON CONFLICT (\"id\") DO UPDATE SET \"status\" = EXCLUDED.\"status\", \"order_date\" = EXCLUDED.\"order_date\", \"amount\" = EXCLUDED.\"amount\""
        );
        assert_eq!(
            writer.delete_sql.as_deref(),
            Some(
                "DELETE FROM \"public\".\"orders_copy\" AS t USING \"floe_sink_stage_deletes\" AS s WHERE \"t\".\"id\" = \"s\".\"c0\"::bigint"
            )
        );

        let amounts = Decimal128Array::from(vec![Some(12345_i128), Some(999_i128)])
            .with_precision_and_scale(12, 2)
            .expect("decimal metadata");
        let batch = MvChangelogBatch {
            version: 42,
            version_time: None,
            kind: MvChangelogBatchKind::Delta,
            batch: RecordBatch::try_new(
                arrow_schema,
                vec![
                    Arc::new(Int64Array::from(vec![1, 1])),
                    Arc::new(StringArray::from(vec![Some("old"), Some("new")])),
                    Arc::new(Date32Array::from(vec![Some(19_358), Some(19_359)])),
                    Arc::new(amounts),
                ],
            )
            .expect("record batch"),
            diffs: vec![-1, 1],
        };

        let actions = writer.batch_actions(&batch).expect("actions");
        assert_eq!(
            actions.deletes,
            vec![vec![PostgresParamValue::Int64(Some(1))]]
        );
        assert_eq!(
            actions.inserts,
            vec![vec![
                PostgresParamValue::Int64(Some(1)),
                PostgresParamValue::Text(Some("new".to_string())),
                PostgresParamValue::Int32(Some(19_359)),
                PostgresParamValue::Text(Some("9.99".to_string())),
            ]]
        );
    }

    #[test]
    fn postgres_stage_copy_rows_quote_values_and_nulls() {
        let row = vec![
            PostgresParamValue::Int64(Some(7)),
            PostgresParamValue::Text(Some("a,\"b\"\nnext".to_string())),
            PostgresParamValue::Text(None),
            PostgresParamValue::Bool(Some(true)),
        ];
        let mut encoded = String::new();

        append_copy_row(&mut encoded, &row, Some(3));

        assert_eq!(encoded, "\"7\",\"a,\"\"b\"\"\nnext\",\\N,\"true\",3\n");
    }

    #[test]
    fn postgres_append_only_sink_rejects_negative_diffs() {
        let arrow_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let schema = PostgresSinkSchema::from_arrow(
            Arc::clone(&arrow_schema),
            vec![],
            PostgresSinkMode::AppendOnly,
        )
        .expect("schema");
        let writer = PostgresSinkWriter::new(
            "postgres://postgres:postgres@localhost/postgres",
            "public.events",
            PostgresSinkMode::AppendOnly,
            schema,
        )
        .expect("writer");
        let batch = MvChangelogBatch {
            version: 7,
            version_time: None,
            kind: MvChangelogBatchKind::Delta,
            batch: RecordBatch::try_new(arrow_schema, vec![Arc::new(Int64Array::from(vec![1]))])
                .expect("record batch"),
            diffs: vec![-1],
        };

        let err = writer
            .batch_actions(&batch)
            .expect_err("negative diff fails");
        assert!(
            err.to_string()
                .contains("Postgres append-only sink received negative diff")
        );
    }
}
