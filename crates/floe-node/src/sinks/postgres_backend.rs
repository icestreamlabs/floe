use std::collections::HashSet;

use anyhow::ensure;
use futures::StreamExt;
use tokio::task::JoinHandle;
use tokio_postgres::types::ToSql;

use super::*;

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

pub(super) async fn run_postgres_sink(
    sink_name: &str,
    registry: Arc<MaterializedViewRegistry>,
    cancel: CancellationToken,
    connection: &str,
    table: &str,
    mv: &str,
    mode: Option<&str>,
    primary_key: Vec<String>,
    with_snapshot: bool,
    as_of: Option<i64>,
    retry_policy: RetryPolicy,
    checkpoint_tx: Option<mpsc::UnboundedSender<SinkCursor>>,
) -> Result<()> {
    let mode = PostgresSinkMode::parse(mode)?;
    let mut stream = execute_mv_changelog(
        registry.as_ref(),
        MvChangelogParams {
            mv_name: mv.to_string(),
            with_snapshot,
            as_of,
        },
        cancel,
    )
    .await?;
    let schema = PostgresSinkSchema::from_arrow(stream.schema(), primary_key, mode)?;
    let mut writer = PostgresSinkWriter::new(connection, table, mode, schema)?;

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        apply_postgres_batch_with_retry(&mut writer, &batch, retry_policy)
            .await
            .with_context(|| {
                format!(
                    "apply MV changelog version {} to Postgres sink '{sink_name}'",
                    batch.version
                )
            })?;
        publish_sink_cursor(
            &checkpoint_tx,
            SinkCursor {
                sink: sink_name.to_string(),
                mv_name: mv.to_string(),
                last_emitted_mv_version: batch.version,
                row_index: None,
            },
        );
    }

    Ok(())
}

async fn apply_postgres_batch_with_retry(
    writer: &mut PostgresSinkWriter,
    batch: &MvChangelogBatch,
    retry_policy: RetryPolicy,
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
                tokio::time::sleep(backoff).await;
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
            PostgresSinkMode::Upsert => postgres_upsert_sql(&schema, table)?,
            PostgresSinkMode::AppendOnly => postgres_append_insert_sql(&schema, table)?,
        };
        let delete_sql = match mode {
            PostgresSinkMode::Upsert => Some(postgres_delete_sql(&schema, table)?),
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
        let insert_statement = transaction
            .prepare(&insert_sql)
            .await
            .with_context(|| format!("prepare Postgres sink insert for {target_table}"))?;
        let delete_statement = if let Some(delete_sql) = &delete_sql {
            Some(
                transaction
                    .prepare(delete_sql)
                    .await
                    .with_context(|| format!("prepare Postgres sink delete for {target_table}"))?,
            )
        } else {
            None
        };

        if !actions.deletes.is_empty() {
            let delete_statement = delete_statement
                .as_ref()
                .context("Postgres upsert sink is missing delete statement")?;
            for params in &actions.deletes {
                let refs = params
                    .iter()
                    .map(PostgresParamValue::as_tosql)
                    .collect::<Vec<_>>();
                transaction
                    .execute(delete_statement, &refs)
                    .await
                    .with_context(|| format!("delete MV row from Postgres sink {target_table}"))?;
            }
        }

        for params in &actions.inserts {
            let refs = params
                .iter()
                .map(PostgresParamValue::as_tosql)
                .collect::<Vec<_>>();
            transaction
                .execute(&insert_statement, &refs)
                .await
                .with_context(|| match mode {
                    PostgresSinkMode::Upsert => {
                        format!("upsert MV row into Postgres sink {target_table}")
                    }
                    PostgresSinkMode::AppendOnly => {
                        format!("insert MV row into append-only Postgres sink {target_table}")
                    }
                })?;
        }

        transaction
            .commit()
            .await
            .with_context(|| format!("commit Postgres sink transaction for {target_table}"))
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
        Ok(())
    }

    fn disconnect(&mut self) {
        self.connection = None;
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
    Float64(Option<f64>),
    Int32(Option<i32>),
}

impl PostgresParamValue {
    fn null(data_type: &ColumnType) -> Self {
        match data_type {
            ColumnType::Int64 => Self::Int64(None),
            ColumnType::Bool => Self::Bool(None),
            ColumnType::Utf8 => Self::Text(None),
            ColumnType::TimestampMillis => Self::Float64(None),
            ColumnType::DateDays => Self::Int32(None),
            ColumnType::Decimal128 { .. } | ColumnType::Numeric => Self::Text(None),
        }
    }

    fn as_tosql(&self) -> &(dyn ToSql + Sync) {
        match self {
            Self::Int64(value) => value,
            Self::Bool(value) => value,
            Self::Text(value) => value,
            Self::Float64(value) => value,
            Self::Int32(value) => value,
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
            Ok(PostgresParamValue::Float64(Some(value as f64)))
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

fn postgres_append_insert_sql(schema: &PostgresSinkSchema, table: &str) -> Result<String> {
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
        .map(|(idx, column)| postgres_value_expr(idx + 1, &column.data_type))
        .collect::<Vec<_>>();
    Ok(format!(
        "INSERT INTO {table} ({}) VALUES ({})",
        columns.join(", "),
        values.join(", ")
    ))
}

fn postgres_upsert_sql(schema: &PostgresSinkSchema, table: &str) -> Result<String> {
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
        .map(|(idx, column)| postgres_value_expr(idx + 1, &column.data_type))
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
    Ok(format!(
        "INSERT INTO {table} ({}) VALUES ({}) ON CONFLICT ({}) {conflict_action}",
        columns.join(", "),
        values.join(", "),
        primary_keys.join(", ")
    ))
}

fn postgres_delete_sql(schema: &PostgresSinkSchema, table: &str) -> Result<String> {
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
                quote_postgres_ident(&column.name),
                postgres_value_expr(idx + 1, &column.data_type)
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(format!(
        "DELETE FROM {table} WHERE {}",
        predicates.join(" AND ")
    ))
}

fn postgres_value_expr(param_idx: usize, data_type: &ColumnType) -> String {
    match data_type {
        ColumnType::TimestampMillis => {
            format!("to_timestamp(${param_idx}::double precision / 1000.0)")
        }
        ColumnType::DateDays => format!("DATE '1970-01-01' + ${param_idx}::integer"),
        ColumnType::Decimal128 { .. } | ColumnType::Numeric => {
            format!("${param_idx}::numeric")
        }
        ColumnType::Int64 | ColumnType::Bool | ColumnType::Utf8 => format!("${param_idx}"),
    }
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
            "INSERT INTO \"public\".\"orders_copy\" (\"id\", \"status\", \"order_date\", \"amount\") VALUES ($1, $2, DATE '1970-01-01' + $3::integer, $4::numeric) ON CONFLICT (\"id\") DO UPDATE SET \"status\" = EXCLUDED.\"status\", \"order_date\" = EXCLUDED.\"order_date\", \"amount\" = EXCLUDED.\"amount\""
        );
        assert_eq!(
            writer.delete_sql.as_deref(),
            Some("DELETE FROM \"public\".\"orders_copy\" WHERE \"id\" = $1")
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
