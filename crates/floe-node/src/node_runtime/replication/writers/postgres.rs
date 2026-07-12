use super::*;
use crate::postgres_sql::{quote_postgres_ident, quote_postgres_qualified_name};
use floe_core::decimal::format_decimal128;
use floe_core::postgres_types::{normalize_postgres_type, postgres_type_compatible};

pub(in crate::node_runtime::replication) struct PostgresReplicationPipelineWriter {
    connection: String,
    target_table_name: String,
    target_table: String,
    pub(in crate::node_runtime::replication) insert_sql: String,
    pub(in crate::node_runtime::replication) delete_sql: String,
    schema: CdcTableSchema,
}

#[derive(Clone, Debug, PartialEq)]
pub(in crate::node_runtime::replication) enum PostgresParamValue {
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

impl PostgresReplicationPipelineWriter {
    pub(in crate::node_runtime::replication) fn new(
        connection: &str,
        table: &str,
        schema: CdcTableSchema,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !connection.trim().is_empty(),
            "replication Postgres connection cannot be empty"
        );
        let target_table = quote_postgres_qualified_name(table)?;
        encoding::validate_floe_json_schema(&schema)?;
        Ok(Self {
            connection: connection.to_string(),
            target_table_name: table.to_string(),
            target_table,
            insert_sql: postgres_upsert_sql(&schema, table)?,
            delete_sql: postgres_delete_sql(&schema, table)?,
            schema,
        })
    }

    pub(in crate::node_runtime::replication) async fn send_records(
        &self,
        records: &[CdcBufferRecord],
    ) -> anyhow::Result<std::collections::BTreeMap<String, String>> {
        if records.is_empty() {
            return Ok(self.target_state(0));
        }

        let (mut client, connection) =
            tokio_postgres::connect(self.connection.as_str(), tokio_postgres::NoTls)
                .await
                .context("connect replication pipeline Postgres target")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(
                    error = %err,
                    "replication pipeline Postgres target connection failed"
                );
            }
        });
        let target_info = self.validate_target_compatibility(&client).await?;
        let dynamic_insert_sql = postgres_upsert_sql_with_target(
            &self.schema,
            &self.target_table_name,
            Some(&target_info),
        )?;
        let dynamic_delete_sql = postgres_delete_sql_with_target(
            &self.schema,
            &self.target_table_name,
            Some(&target_info),
        )?;
        let insert_sql = if dynamic_insert_sql == self.insert_sql {
            self.insert_sql.as_str()
        } else {
            dynamic_insert_sql.as_str()
        };
        let delete_sql = if dynamic_delete_sql == self.delete_sql {
            self.delete_sql.as_str()
        } else {
            dynamic_delete_sql.as_str()
        };

        let transaction = client
            .transaction()
            .await
            .context("start replication pipeline Postgres target transaction")?;
        let insert_statement = transaction.prepare(insert_sql).await.with_context(|| {
            format!(
                "prepare CDC upsert statement for replication pipeline Postgres target {}",
                self.target_table
            )
        })?;
        let delete_statement = transaction.prepare(delete_sql).await.with_context(|| {
            format!(
                "prepare CDC delete statement for replication pipeline Postgres target {}",
                self.target_table
            )
        })?;
        for record in records {
            self.apply_record(&transaction, record, &insert_statement, &delete_statement)
                .await?;
        }
        transaction
            .commit()
            .await
            .context("commit replication pipeline Postgres target transaction")?;
        Ok(self.target_state(records.len()))
    }

    async fn apply_record(
        &self,
        transaction: &tokio_postgres::Transaction<'_>,
        record: &CdcBufferRecord,
        insert_statement: &Statement,
        delete_statement: &Statement,
    ) -> anyhow::Result<()> {
        let value = parse_floe_json_record_value(record)?;
        let deleted = value
            .get(FLOE_JSON_DELETED_FIELD)
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        if deleted {
            let key = parse_floe_json_record_key(record).unwrap_or_else(|_| value.clone());
            let params = postgres_key_params_from_json(&self.schema, &key)?;
            let refs = params
                .iter()
                .map(PostgresParamValue::as_tosql)
                .collect::<Vec<_>>();
            transaction
                .execute(delete_statement, &refs)
                .await
                .with_context(|| {
                    format!(
                        "delete CDC row from replication pipeline Postgres target {}",
                        self.target_table
                    )
                })?;
            return Ok(());
        }

        let params = postgres_row_params_from_json(&self.schema, &value)?;
        let refs = params
            .iter()
            .map(PostgresParamValue::as_tosql)
            .collect::<Vec<_>>();
        transaction
            .execute(insert_statement, &refs)
            .await
            .with_context(|| {
                format!(
                    "upsert CDC row into replication pipeline Postgres target {}",
                    self.target_table
                )
            })?;
        Ok(())
    }

    fn target_state(&self, records: usize) -> std::collections::BTreeMap<String, String> {
        let mut target_state = TargetStateBuilder::new();
        target_state
            .postgres_table(&self.target_table)
            .postgres_records_applied(records);
        target_state.build()
    }

    async fn validate_target_compatibility(
        &self,
        client: &tokio_postgres::Client,
    ) -> anyhow::Result<PostgresTargetTableInfo> {
        let target_info = load_postgres_target_table_info(client, &self.target_table_name).await?;
        validate_postgres_target_table_compatibility(
            &self.schema,
            &self.target_table_name,
            &target_info,
        )?;
        Ok(target_info)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::node_runtime::replication) struct PostgresTargetColumnInfo {
    name: String,
    postgres_type: String,
    numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
    not_null: bool,
    has_default: bool,
    generated: bool,
}

impl PostgresTargetColumnInfo {
    pub(in crate::node_runtime::replication) fn new(
        name: impl Into<String>,
        postgres_type: impl Into<String>,
        not_null: bool,
        has_default: bool,
        generated: bool,
    ) -> Self {
        Self {
            name: name.into(),
            postgres_type: postgres_type.into(),
            numeric_precision: None,
            numeric_scale: None,
            not_null,
            has_default,
            generated,
        }
    }

    pub(in crate::node_runtime::replication) fn with_numeric_shape(
        mut self,
        precision: Option<i32>,
        scale: Option<i32>,
    ) -> Self {
        self.numeric_precision = precision;
        self.numeric_scale = scale;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::node_runtime::replication) struct PostgresTargetTableInfo {
    columns: Vec<PostgresTargetColumnInfo>,
    unique_indexes: Vec<Vec<String>>,
}

impl PostgresTargetTableInfo {
    pub(in crate::node_runtime::replication) fn new(
        columns: Vec<PostgresTargetColumnInfo>,
        unique_indexes: Vec<Vec<String>>,
    ) -> Self {
        Self {
            columns,
            unique_indexes,
        }
    }

    fn column(&self, name: &str) -> Option<&PostgresTargetColumnInfo> {
        self.columns
            .iter()
            .find(|target_column| target_column.name == name)
    }
}

async fn load_postgres_target_table_info(
    client: &tokio_postgres::Client,
    table: &str,
) -> anyhow::Result<PostgresTargetTableInfo> {
    let table_exists = client
        .query_one("SELECT to_regclass($1)::text", &[&table])
        .await
        .with_context(|| format!("look up replication pipeline Postgres target table {table}"))?;
    let regclass: Option<String> = table_exists.get(0);
    anyhow::ensure!(
        regclass.is_some(),
        "replication pipeline Postgres target table '{table}' does not exist"
    );

    let column_rows = client
        .query(
            "
            SELECT
                a.attname,
                a.atttypid::regtype::text,
                CASE
                    WHEN a.atttypid = 'numeric'::regtype AND a.atttypmod >= 0
                    THEN ((a.atttypmod - 4) >> 16) & 65535
                END AS numeric_precision,
                CASE
                    WHEN a.atttypid = 'numeric'::regtype AND a.atttypmod >= 0
                    THEN (a.atttypmod - 4) & 2047
                END AS numeric_scale,
                a.attnotnull,
                d.adbin IS NOT NULL AS has_default,
                a.attgenerated <> '' AS generated
            FROM pg_attribute a
            LEFT JOIN pg_attrdef d
                ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE a.attrelid = to_regclass($1)
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            ",
            &[&table],
        )
        .await
        .with_context(|| {
            format!("load replication pipeline Postgres target table {table} columns")
        })?;
    let columns = column_rows
        .into_iter()
        .map(|row| {
            PostgresTargetColumnInfo::new(
                row.get::<_, String>(0),
                row.get::<_, String>(1),
                row.get::<_, bool>(4),
                row.get::<_, bool>(5),
                row.get::<_, bool>(6),
            )
            .with_numeric_shape(row.get(2), row.get(3))
        })
        .collect::<Vec<_>>();

    let unique_rows = client
        .query(
            "
            SELECT array_agg(a.attname ORDER BY indexed_columns.ord)::text[] AS columns
            FROM pg_index i
            JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS indexed_columns(attnum, ord)
                ON true
            JOIN pg_attribute a
                ON a.attrelid = i.indrelid AND a.attnum = indexed_columns.attnum
            WHERE i.indrelid = to_regclass($1)
              AND i.indisunique
              AND i.indpred IS NULL
              AND i.indexprs IS NULL
            GROUP BY i.indexrelid
            ",
            &[&table],
        )
        .await
        .with_context(|| {
            format!("load replication pipeline Postgres target table {table} unique indexes")
        })?;
    let unique_indexes = unique_rows
        .into_iter()
        .map(|row| row.get::<_, Vec<String>>(0))
        .collect::<Vec<_>>();

    Ok(PostgresTargetTableInfo::new(columns, unique_indexes))
}

pub(in crate::node_runtime::replication) fn validate_postgres_target_table_compatibility(
    schema: &CdcTableSchema,
    table: &str,
    target: &PostgresTargetTableInfo,
) -> anyhow::Result<()> {
    let mut seen_cdc_columns = HashSet::new();
    for column in schema.columns() {
        seen_cdc_columns.insert(column.name());
        let Some(target_column) = target.column(column.name()) else {
            return Err(anyhow!(
                "replication pipeline Postgres target table '{table}' is missing CDC column '{}'; add the column or point the pipeline at a migrated table before resuming",
                column.name()
            ));
        };
        anyhow::ensure!(
            postgres_type_compatible(
                column.data_type(),
                &target_column.postgres_type,
                &target_column.postgres_type,
                target_column.numeric_precision,
                target_column.numeric_scale,
            ),
            "replication pipeline Postgres target table '{table}' column '{}' has type '{}' but CDC schema expects {:?}; migrate the target column before resuming",
            column.name(),
            target_column.postgres_type,
            column.data_type()
        );
        anyhow::ensure!(
            !column.nullable() || !target_column.not_null,
            "replication pipeline Postgres target table '{table}' column '{}' is NOT NULL but CDC schema allows NULL; relax the target constraint or backfill a non-null source contract before resuming",
            column.name()
        );
    }

    for target_column in &target.columns {
        if seen_cdc_columns.contains(target_column.name.as_str()) {
            continue;
        }
        anyhow::ensure!(
            !target_column.not_null || target_column.has_default || target_column.generated,
            "replication pipeline Postgres target table '{table}' has required column '{}' that is not present in the CDC schema; add a default/generated expression, make it nullable, or include it in the CDC source before resuming",
            target_column.name
        );
    }

    let primary_key = schema.primary_key().columns().to_vec();
    anyhow::ensure!(
        target
            .unique_indexes
            .iter()
            .any(|unique_index| unique_index == &primary_key),
        "replication pipeline Postgres target table '{table}' has no unique index matching CDC primary key {:?}; create the matching primary key or unique index before resuming",
        primary_key
    );
    Ok(())
}

pub(in crate::node_runtime::replication) fn parse_floe_json_record_value(
    record: &CdcBufferRecord,
) -> anyhow::Result<serde_json::Map<String, serde_json::Value>> {
    let value = record
        .value()
        .ok_or_else(|| anyhow!("Floe JSON Postgres target record is missing a value"))?;
    let value = serde_json::from_slice::<serde_json::Value>(value)
        .context("parse Floe JSON Postgres target record value")?;
    value
        .as_object()
        .cloned()
        .ok_or_else(|| anyhow!("Floe JSON Postgres target record value must be an object"))
}

pub(in crate::node_runtime::replication) fn parse_floe_json_record_key(
    record: &CdcBufferRecord,
) -> anyhow::Result<serde_json::Map<String, serde_json::Value>> {
    let key = record
        .key()
        .ok_or_else(|| anyhow!("Floe JSON Postgres target record is missing a key"))?;
    let key = serde_json::from_slice::<serde_json::Value>(key)
        .context("parse Floe JSON Postgres target record key")?;
    key.as_object()
        .cloned()
        .ok_or_else(|| anyhow!("Floe JSON Postgres target record key must be an object"))
}

pub(in crate::node_runtime::replication) fn postgres_row_params_from_json(
    schema: &CdcTableSchema,
    object: &serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<Vec<PostgresParamValue>> {
    schema
        .columns()
        .iter()
        .map(|column| postgres_param_from_json(column, object.get(column.name())))
        .collect()
}

pub(in crate::node_runtime::replication) fn postgres_key_params_from_json(
    schema: &CdcTableSchema,
    object: &serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<Vec<PostgresParamValue>> {
    schema
        .primary_key()
        .columns()
        .iter()
        .map(|column_name| {
            let column = schema
                .columns()
                .iter()
                .find(|column| column.name() == column_name)
                .ok_or_else(|| {
                    anyhow!("CDC primary-key column '{column_name}' missing from schema")
                })?;
            postgres_param_from_json(column, object.get(column.name()))
        })
        .collect()
}

fn postgres_param_from_json(
    column: &CdcColumn,
    value: Option<&serde_json::Value>,
) -> anyhow::Result<PostgresParamValue> {
    let Some(value) = value else {
        anyhow::ensure!(
            column.nullable(),
            "CDC column '{}' is required for Postgres target",
            column.name()
        );
        return Ok(PostgresParamValue::null(column.data_type()));
    };
    if value.is_null() {
        anyhow::ensure!(
            column.nullable(),
            "CDC column '{}' cannot be NULL for Postgres target",
            column.name()
        );
        return Ok(PostgresParamValue::null(column.data_type()));
    }
    match column.data_type() {
        ColumnType::Int64 => Ok(PostgresParamValue::Int64(Some(json_i64(
            column.name(),
            value,
        )?))),
        ColumnType::Bool => Ok(PostgresParamValue::Bool(Some(json_bool(
            column.name(),
            value,
        )?))),
        ColumnType::Utf8 => Ok(PostgresParamValue::Text(Some(json_string(
            column.name(),
            value,
        )?))),
        ColumnType::TimestampMillis => Ok(PostgresParamValue::Float64(Some(json_i64(
            column.name(),
            value,
        )? as f64))),
        ColumnType::DateDays => Ok(PostgresParamValue::Int32(Some(json_i32(
            column.name(),
            value,
        )?))),
        ColumnType::Decimal128 { scale, .. } => {
            let text = if let Some(value) = value.as_str() {
                value.to_string()
            } else {
                format_decimal128(i128::from(json_i64(column.name(), value)?), *scale)?
            };
            Ok(PostgresParamValue::Text(Some(text)))
        }
        ColumnType::Numeric => Ok(PostgresParamValue::Text(Some(json_scalar_string(
            column.name(),
            value,
        )?))),
    }
}

fn json_i64(column: &str, value: &serde_json::Value) -> anyhow::Result<i64> {
    if let Some(value) = value.as_i64() {
        return Ok(value);
    }
    if let Some(value) = value.as_str() {
        return value
            .parse::<i64>()
            .with_context(|| format!("parse CDC column '{column}' as i64"));
    }
    Err(anyhow!("CDC column '{column}' must be an integer"))
}

fn json_i32(column: &str, value: &serde_json::Value) -> anyhow::Result<i32> {
    let value = json_i64(column, value)?;
    i32::try_from(value).with_context(|| format!("CDC column '{column}' exceeds i32 range"))
}

fn json_bool(column: &str, value: &serde_json::Value) -> anyhow::Result<bool> {
    if let Some(value) = value.as_bool() {
        return Ok(value);
    }
    if let Some(value) = value.as_str() {
        return value
            .parse::<bool>()
            .with_context(|| format!("parse CDC column '{column}' as bool"));
    }
    Err(anyhow!("CDC column '{column}' must be a boolean"))
}

fn json_string(column: &str, value: &serde_json::Value) -> anyhow::Result<String> {
    value
        .as_str()
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("CDC column '{column}' must be a string"))
}

fn json_scalar_string(column: &str, value: &serde_json::Value) -> anyhow::Result<String> {
    match value {
        serde_json::Value::String(value) => Ok(value.clone()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        serde_json::Value::Bool(value) => Ok(value.to_string()),
        _ => Err(anyhow!("CDC column '{column}' must be a scalar value")),
    }
}

fn postgres_upsert_sql(schema: &CdcTableSchema, table: &str) -> anyhow::Result<String> {
    postgres_upsert_sql_with_target(schema, table, None)
}

pub(in crate::node_runtime::replication) fn postgres_upsert_sql_with_target(
    schema: &CdcTableSchema,
    table: &str,
    target: Option<&PostgresTargetTableInfo>,
) -> anyhow::Result<String> {
    let table = quote_postgres_qualified_name(table)?;
    let columns = schema
        .columns()
        .iter()
        .map(|column| quote_postgres_ident(column.name()))
        .collect::<Vec<_>>();
    let values = schema
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            postgres_value_expr(
                idx + 1,
                column.data_type(),
                target.and_then(|target| target.column(column.name())),
            )
        })
        .collect::<Vec<_>>();
    let primary_keys = schema
        .primary_key()
        .columns()
        .iter()
        .map(|column| quote_postgres_ident(column))
        .collect::<Vec<_>>();
    let primary_key_names = schema
        .primary_key()
        .columns()
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let updates = schema
        .columns()
        .iter()
        .filter(|column| !primary_key_names.contains(column.name()))
        .map(|column| {
            let quoted = quote_postgres_ident(column.name());
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

fn postgres_delete_sql(schema: &CdcTableSchema, table: &str) -> anyhow::Result<String> {
    postgres_delete_sql_with_target(schema, table, None)
}

pub(in crate::node_runtime::replication) fn postgres_delete_sql_with_target(
    schema: &CdcTableSchema,
    table: &str,
    target: Option<&PostgresTargetTableInfo>,
) -> anyhow::Result<String> {
    let table = quote_postgres_qualified_name(table)?;
    let predicates = schema
        .primary_key()
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, column_name)| {
            let column = schema
                .columns()
                .iter()
                .find(|column| column.name() == column_name)
                .ok_or_else(|| {
                    anyhow!("CDC primary-key column '{column_name}' missing from schema")
                })?;
            Ok(format!(
                "{} = {}",
                quote_postgres_ident(column.name()),
                postgres_value_expr(
                    idx + 1,
                    column.data_type(),
                    target.and_then(|target| target.column(column.name())),
                )
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(format!(
        "DELETE FROM {table} WHERE {}",
        predicates.join(" AND ")
    ))
}

fn postgres_value_expr(
    param_idx: usize,
    data_type: &ColumnType,
    target_column: Option<&PostgresTargetColumnInfo>,
) -> String {
    match data_type {
        ColumnType::TimestampMillis => {
            format!("to_timestamp(${param_idx}::double precision / 1000.0)")
        }
        ColumnType::DateDays => format!("DATE '1970-01-01' + ${param_idx}::integer"),
        ColumnType::Decimal128 { .. } | ColumnType::Numeric => {
            format!("${param_idx}::numeric")
        }
        ColumnType::Utf8 => match target_column
            .map(|column| normalize_postgres_type(&column.postgres_type))
            .as_deref()
        {
            Some("uuid") => format!("${param_idx}::uuid"),
            Some("json") => format!("${param_idx}::json"),
            Some("jsonb") => format!("${param_idx}::jsonb"),
            Some("bytea") => format!("${param_idx}::bytea"),
            _ => format!("${param_idx}"),
        },
        ColumnType::Int64 | ColumnType::Bool => format!("${param_idx}"),
    }
}
