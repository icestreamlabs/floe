use super::*;

use floe_core::decimal::parse_decimal_text_to_i128;

pub(super) async fn snapshot_table_chunks(
    transaction: &tokio_postgres::Transaction<'_>,
    schema: &CdcTableSchema,
    settings: PostgresCdcSnapshotConfig,
) -> Result<Vec<SnapshotTableChunk>> {
    let requested_chunks = settings.intra_table_chunks.max(1);
    if requested_chunks <= 1 {
        return Ok(vec![SnapshotTableChunk::Full]);
    }

    let Some(key_column) = single_int64_primary_key_column(schema) else {
        tracing::debug!(
            table = %schema.table_id().as_str(),
            upstream_schema = %schema.upstream_table().schema(),
            upstream_table = %schema.upstream_table().table(),
            requested_chunks,
            "Postgres CDC snapshot intra-table chunking skipped because the primary key is not a single Int64 column"
        );
        return Ok(vec![SnapshotTableChunk::Full]);
    };

    let Some((min_key, max_key)) =
        snapshot_int64_primary_key_bounds(transaction, schema, key_column.name()).await?
    else {
        return Ok(vec![SnapshotTableChunk::Full]);
    };

    Ok(int64_snapshot_range_chunks(
        key_column.name(),
        min_key,
        max_key,
        requested_chunks,
    ))
}

pub(super) async fn snapshot_int64_primary_key_bounds(
    transaction: &tokio_postgres::Transaction<'_>,
    schema: &CdcTableSchema,
    key_column: &str,
) -> Result<Option<(i64, i64)>> {
    let quoted_key = quote_pg_ident(key_column);
    let query = format!(
        "SELECT min({quoted_key})::bigint, max({quoted_key})::bigint FROM {}",
        qualified_table_name(schema.upstream_table())
    );
    let row = transaction.query_one(&query, &[]).await.with_context(|| {
        format!(
            "discover Postgres CDC snapshot key bounds for '{}.{}'",
            schema.upstream_table().schema(),
            schema.upstream_table().table()
        )
    })?;
    let min_key: Option<i64> = row.get(0);
    let max_key: Option<i64> = row.get(1);
    Ok(min_key.zip(max_key))
}

pub(super) fn single_int64_primary_key_column(schema: &CdcTableSchema) -> Option<&CdcColumn> {
    let [primary_key_column] = schema.primary_key().columns() else {
        return None;
    };
    let column_idx = schema.column_index(primary_key_column)?;
    let column = &schema.columns()[column_idx];
    (column.data_type() == &ColumnType::Int64).then_some(column)
}

pub(super) fn int64_snapshot_range_chunks(
    column: &str,
    min_key: i64,
    max_key: i64,
    requested_chunks: usize,
) -> Vec<SnapshotTableChunk> {
    if requested_chunks <= 1 || min_key >= max_key {
        return vec![SnapshotTableChunk::Full];
    }

    let value_count = i128::from(max_key) - i128::from(min_key) + 1;
    let chunk_count = (requested_chunks as i128).min(value_count).max(1);
    if chunk_count <= 1 {
        return vec![SnapshotTableChunk::Full];
    }

    let width = (value_count + chunk_count - 1) / chunk_count;
    let mut chunks = Vec::with_capacity(usize::try_from(chunk_count).unwrap_or(usize::MAX));
    for idx in 0..chunk_count {
        let lower = i128::from(min_key) + idx * width;
        if lower > i128::from(max_key) {
            break;
        }
        let next = lower + width;
        let upper_exclusive = (next <= i128::from(max_key))
            .then(|| i64::try_from(next).expect("chunk upper bound remains in i64 range"));
        chunks.push(SnapshotTableChunk::Int64Range {
            column: column.to_string(),
            lower_inclusive: i64::try_from(lower).expect("chunk lower bound remains in i64 range"),
            upper_exclusive,
        });
    }
    chunks
}

pub(super) async fn snapshot_table_change_batches(
    transaction: &tokio_postgres::Transaction<'_>,
    schema: &CdcTableSchema,
    settings: PostgresCdcSnapshotConfig,
) -> Result<SnapshotTableChangeBatches> {
    let chunk = SnapshotTableChunk::Full;
    snapshot_table_change_batches_for_chunk(transaction, schema, &chunk, settings).await
}

pub(super) async fn snapshot_table_change_batches_for_chunk(
    transaction: &tokio_postgres::Transaction<'_>,
    schema: &CdcTableSchema,
    chunk: &SnapshotTableChunk,
    settings: PostgresCdcSnapshotConfig,
) -> Result<SnapshotTableChangeBatches> {
    let query = snapshot_table_query(schema, chunk);
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
    let rows_per_batch = settings.rows_per_batch.max(1);
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
    if settings.perf_log {
        let elapsed = started_at.elapsed();
        tracing::info!(
            table = %schema.table_id().as_str(),
            upstream_schema = %schema.upstream_table().schema(),
            upstream_table = %schema.upstream_table().table(),
            chunk = ?chunk,
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

pub(super) async fn snapshot_table_change_batches_from_exported_snapshot(
    connection_string: &str,
    exported_snapshot: &str,
    schema: &CdcTableSchema,
    chunk: &SnapshotTableChunk,
    settings: PostgresCdcSnapshotConfig,
    worker_control: Option<SnapshotWorkerControl>,
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
        bind_transaction_to_exported_snapshot(&transaction, exported_snapshot).await?;
        let scan_permit = if let Some(control) = worker_control {
            let SnapshotWorkerControl {
                ready_tx,
                mut start_rx,
                scan_limiter,
                scan_observation_tx,
            } = control;
            let _ = ready_tx.send(());
            wait_for_snapshot_worker_start(&mut start_rx).await?;
            Some((scan_limiter.acquire().await, scan_observation_tx))
        } else {
            None
        };
        let scan_started_at = Instant::now();
        let snapshot =
            snapshot_table_change_batches_for_chunk(&transaction, schema, chunk, settings).await;
        if let (Some((_, Some(scan_observation_tx))), Ok(snapshot)) = (&scan_permit, &snapshot) {
            let _ = scan_observation_tx.send(Some(SnapshotScanObservation {
                elapsed_ms: scan_started_at
                    .elapsed()
                    .as_millis()
                    .try_into()
                    .unwrap_or(u64::MAX),
                rows: snapshot.row_count,
            }));
        }
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

pub(super) async fn wait_for_snapshot_worker_start(
    start_rx: &mut watch::Receiver<bool>,
) -> Result<()> {
    loop {
        if *start_rx.borrow_and_update() {
            return Ok(());
        }
        start_rx
            .changed()
            .await
            .context("Postgres snapshot worker start channel closed before WAL stream started")?;
    }
}

pub(super) async fn bind_transaction_to_exported_snapshot(
    transaction: &tokio_postgres::Transaction<'_>,
    exported_snapshot: &str,
) -> Result<()> {
    transaction
        .batch_execute(&format!(
            "SET TRANSACTION SNAPSHOT {}",
            quote_pg_literal(exported_snapshot)
        ))
        .await
        .context("bind Postgres transaction to exported snapshot")
}

pub(super) fn snapshot_table_query(schema: &CdcTableSchema, chunk: &SnapshotTableChunk) -> String {
    let select_list = schema
        .columns()
        .iter()
        .map(snapshot_select_expr)
        .collect::<Vec<_>>()
        .join(", ");
    let base = format!(
        "SELECT {select_list} FROM {}",
        qualified_table_name(schema.upstream_table())
    );
    match chunk {
        SnapshotTableChunk::Full => base,
        SnapshotTableChunk::Int64Range {
            column,
            lower_inclusive,
            upper_exclusive,
        } => {
            let quoted_column = quote_pg_ident(column);
            let upper = upper_exclusive
                .map(|upper| format!(" AND {quoted_column} < {upper}"))
                .unwrap_or_default();
            format!("{base} WHERE {quoted_column} >= {lower_inclusive}{upper}")
        }
    }
}

pub(super) fn snapshot_select_expr(column: &CdcColumn) -> String {
    let quoted = quote_pg_ident(column.name());
    match column.data_type() {
        ColumnType::TimestampMillis => {
            format!("floor(extract(epoch from {quoted}) * 1000)::bigint AS {quoted}")
        }
        ColumnType::DateDays => format!("({quoted} - DATE '1970-01-01')::int AS {quoted}"),
        ColumnType::Decimal128 { .. } => format!("{quoted}::text AS {quoted}"),
        ColumnType::Numeric => format!("{quoted}::text AS {quoted}"),
        ColumnType::Utf8 => format!("{quoted}::text AS {quoted}"),
        ColumnType::Int64 | ColumnType::Bool => quoted,
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

pub(super) fn snapshot_int64_value(
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
