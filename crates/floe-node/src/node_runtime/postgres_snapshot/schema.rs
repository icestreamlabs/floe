use super::*;

pub(super) async fn postgres_replication_slot_plugin(
    client: &tokio_postgres::Client,
    slot: &str,
) -> Result<Option<Option<String>>> {
    let row = client
        .query_opt(
            "SELECT plugin
             FROM pg_replication_slots
             WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .with_context(|| format!("check Postgres CDC logical replication slot '{slot}'"))?;
    Ok(row.map(|row| row.get(0)))
}

pub(super) async fn validate_publication_tables(
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

pub(super) async fn validate_upstream_table_schema(
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

pub(in crate::node_runtime) async fn discover_postgres_cdc_table_schema(
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

pub(super) async fn discover_postgres_cdc_table_schema_from_transaction(
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

pub(super) async fn discover_primary_key(
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

pub(super) fn postgres_column_type(
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
        "text" | "varchar" | "bpchar" | "name" | "uuid" | "json" | "jsonb" | "bytea" => {
            Ok(ColumnType::Utf8)
        }
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

pub(super) fn postgres_type_compatible(
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
        ColumnType::Utf8 => matches!(
            udt_name.as_str(),
            "text" | "varchar" | "bpchar" | "name" | "uuid" | "json" | "jsonb" | "bytea"
        ),
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

pub(super) fn decimal128_type_from_precision_scale(
    precision: Option<i32>,
    scale: Option<i32>,
) -> Option<Result<ColumnType>> {
    let (Some(precision), Some(scale)) = (precision, scale) else {
        return None;
    };
    if !(1..=38).contains(&precision) || !(0..=precision).contains(&scale) {
        return None;
    }
    Some(ColumnType::decimal128(precision as u8, scale as i8))
}
