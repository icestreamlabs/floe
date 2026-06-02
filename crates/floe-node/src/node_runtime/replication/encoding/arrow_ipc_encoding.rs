use super::*;

pub(super) fn encode_arrow_ipc_pipeline_records(
    plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    batch: &ChangeBatch,
    transaction: &TransactionBatch,
    settings: ReplicationEncodingConfig,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    let mut records = Vec::new();
    let rows_per_record = settings.arrow_ipc_rows_per_record.max(1);
    let mut builder = ArrowIpcChangeBatchBuilder::new(schema, rows_per_record);
    let is_snapshot = transaction
        .transaction_id()
        .is_some_and(|tx| tx.as_str().starts_with("snapshot:"));
    for (idx, change) in batch.changes().iter().enumerate() {
        let sequence = u64::try_from(idx).unwrap_or(u64::MAX);
        match change {
            CdcChange::Insert { row } => {
                builder.append_row(row, if is_snapshot { "r" } else { "c" }, 1, sequence)?;
            }
            CdcChange::Update { before, after, .. } => {
                if let Some(before) = before {
                    builder.append_row(before, "u_before", -1, sequence)?;
                    flush_arrow_ipc_record_if_full(
                        plan,
                        transaction,
                        &mut builder,
                        &mut records,
                        settings,
                    )?;
                }
                builder.append_row(after, "u", 1, sequence)?;
            }
            CdcChange::Delete { key, before } => match before {
                Some(row) => builder.append_row(row, "d", -1, sequence)?,
                None => {
                    let key = key.as_ref().ok_or_else(|| {
                        anyhow!(
                            "CDC Arrow IPC delete for table '{}' requires a key or before row",
                            schema.table_id().as_str()
                        )
                    })?;
                    let key_row = key_only_row(schema, key)?;
                    builder.append_values(&key_row, "d", -1, sequence)?;
                }
            },
            CdcChange::Truncate => {
                return Err(anyhow!(
                    "CDC Arrow IPC truncate for table '{}' is not supported",
                    schema.table_id().as_str()
                ));
            }
        }
        flush_arrow_ipc_record_if_full(plan, transaction, &mut builder, &mut records, settings)?;
    }
    if !builder.is_empty() {
        records.push(finish_arrow_ipc_record(
            plan,
            transaction,
            &mut builder,
            settings,
        )?);
    }
    Ok(records)
}

pub(super) fn encode_arrow_ipc_snapshot_pipeline_records(
    plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    rows: &CdcColumnarRowBatch,
    transaction: &TransactionBatch,
    settings: ReplicationEncodingConfig,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    schema.validate_columnar_rows(rows)?;
    let mut records = Vec::new();
    let rows_per_record = settings.arrow_ipc_rows_per_record.max(1);
    for start in (0..rows.row_count()).step_by(rows_per_record) {
        let len = rows.row_count().saturating_sub(start).min(rows_per_record);
        let batch = arrow_ipc_snapshot_record_batch(schema, rows, start, len)?;
        records.push(arrow_ipc_record_from_batch(
            plan,
            transaction,
            start / rows_per_record,
            batch,
            settings.arrow_ipc_compression,
        )?);
    }
    Ok(records)
}

fn flush_arrow_ipc_record_if_full(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    builder: &mut ArrowIpcChangeBatchBuilder,
    records: &mut Vec<CdcBufferRecord>,
    settings: ReplicationEncodingConfig,
) -> anyhow::Result<()> {
    if builder.is_full() {
        records.push(finish_arrow_ipc_record(
            plan,
            transaction,
            builder,
            settings,
        )?);
    }
    Ok(())
}

fn finish_arrow_ipc_record(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    builder: &mut ArrowIpcChangeBatchBuilder,
    settings: ReplicationEncodingConfig,
) -> anyhow::Result<CdcBufferRecord> {
    let chunk_idx = builder.chunk_idx();
    let batch = builder.finish()?;
    arrow_ipc_record_from_batch(
        plan,
        transaction,
        chunk_idx,
        batch,
        settings.arrow_ipc_compression,
    )
}

fn arrow_ipc_record_from_batch(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    chunk_idx: usize,
    batch: RecordBatch,
    compression: Option<ReplicationArrowIpcCompressionConfig>,
) -> anyhow::Result<CdcBufferRecord> {
    let mut value = Vec::new();
    {
        let mut writer = arrow_ipc_stream_writer(&mut value, batch.schema().as_ref(), compression)
            .context("create replication Arrow IPC writer")?;
        writer
            .write(&batch)
            .context("write replication Arrow IPC batch")?;
        writer
            .finish()
            .context("finish replication Arrow IPC batch")?;
    }
    let key = format!(
        "{}/{}/{chunk_idx:020}",
        plan.upstream_table,
        source_position_key(transaction.commit_position())
    )
    .into_bytes();
    Ok(CdcBufferRecord::new(Some(key), Some(value)))
}

fn arrow_ipc_stream_writer<'a>(
    value: &'a mut Vec<u8>,
    schema: &ArrowSchema,
    compression: Option<ReplicationArrowIpcCompressionConfig>,
) -> anyhow::Result<StreamWriter<&'a mut Vec<u8>>> {
    let Some(compression) = compression else {
        return StreamWriter::try_new(value, schema)
            .context("create uncompressed Arrow IPC writer");
    };
    let options = IpcWriteOptions::try_new(64, false, MetadataVersion::V5)
        .context("create Arrow IPC writer options")?
        .try_with_compression(Some(arrow_ipc_compression_type(compression)))
        .context("configure Arrow IPC compression")?;
    StreamWriter::try_new_with_options(value, schema, options)
        .with_context(|| format!("create {compression:?} Arrow IPC writer"))
}

fn arrow_ipc_compression_type(
    compression: ReplicationArrowIpcCompressionConfig,
) -> CompressionType {
    match compression {
        ReplicationArrowIpcCompressionConfig::Lz4Frame => CompressionType::LZ4_FRAME,
    }
}

fn arrow_ipc_snapshot_record_batch(
    schema: &CdcTableSchema,
    rows: &CdcColumnarRowBatch,
    start: usize,
    len: usize,
) -> anyhow::Result<RecordBatch> {
    let end = start.saturating_add(len);
    anyhow::ensure!(
        end <= rows.row_count(),
        "CDC Arrow IPC snapshot range {start}..{end} exceeds {} rows",
        rows.row_count()
    );
    let mut arrays = rows
        .columns()
        .iter()
        .zip(schema.columns())
        .map(|(values, column)| arrow_ipc_columnar_array(values, column, start, end))
        .collect::<anyhow::Result<Vec<_>>>()?;

    let mut operations = StringBuilder::with_capacity(len, len);
    let mut diffs = Int64Builder::with_capacity(len);
    let mut sequences = Int64Builder::with_capacity(len);
    for sequence in start..end {
        operations.append_value("r");
        diffs.append_value(1);
        sequences.append_value(i64::try_from(sequence).unwrap_or(i64::MAX));
    }
    arrays.push(Arc::new(operations.finish()));
    arrays.push(Arc::new(diffs.finish()));
    arrays.push(Arc::new(sequences.finish()));

    RecordBatch::try_new(arrow_ipc_schema(schema), arrays)
        .context("build replication Arrow IPC snapshot batch")
}

fn arrow_ipc_columnar_array(
    values: &CdcColumnarColumn,
    column: &CdcColumn,
    start: usize,
    end: usize,
) -> anyhow::Result<ArrayRef> {
    anyhow::ensure!(
        values.data_type() == column.data_type().clone(),
        "CDC Arrow IPC snapshot column '{}' type {:?} does not match {:?}",
        column.name(),
        values.data_type(),
        column.data_type()
    );
    let array: ArrayRef = match values {
        CdcColumnarColumn::Int64(values) => {
            Arc::new(arrow_array::Int64Array::from(values[start..end].to_vec()))
        }
        CdcColumnarColumn::Bool(values) => {
            Arc::new(arrow_array::BooleanArray::from(values[start..end].to_vec()))
        }
        CdcColumnarColumn::Utf8(values) => {
            let mut builder = StringBuilder::with_capacity(end - start, (end - start) * 16);
            for value in &values[start..end] {
                match value {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        CdcColumnarColumn::TimestampMillis(values) => Arc::new(
            arrow_array::TimestampMillisecondArray::from(values[start..end].to_vec()),
        ),
        CdcColumnarColumn::DateDays(values) => {
            Arc::new(arrow_array::Date32Array::from(values[start..end].to_vec()))
        }
        CdcColumnarColumn::Decimal128 {
            precision,
            scale,
            values,
        } => Arc::new(
            Decimal128Array::from(values[start..end].to_vec())
                .with_precision_and_scale(*precision, *scale)
                .context("build Decimal128 Arrow IPC snapshot column")?,
        ),
        CdcColumnarColumn::Numeric(values) => {
            let mut builder = StringBuilder::with_capacity(end - start, (end - start) * 16);
            for value in &values[start..end] {
                match value {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
    };
    Ok(array)
}

fn arrow_ipc_schema(schema: &CdcTableSchema) -> Arc<ArrowSchema> {
    let mut fields = schema
        .columns()
        .iter()
        .map(|column| {
            ArrowField::new(
                column.name(),
                match column.data_type() {
                    ColumnType::Int64 => DataType::Int64,
                    ColumnType::Bool => DataType::Boolean,
                    ColumnType::Utf8 => DataType::Utf8,
                    ColumnType::TimestampMillis => {
                        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None)
                    }
                    ColumnType::DateDays => DataType::Date32,
                    ColumnType::Decimal128 { precision, scale } => {
                        DataType::Decimal128(*precision, *scale)
                    }
                    ColumnType::Numeric => DataType::Utf8,
                },
                true,
            )
        })
        .collect::<Vec<_>>();
    fields.push(ArrowField::new("__op", DataType::Utf8, false));
    fields.push(ArrowField::new("__diff", DataType::Int64, false));
    fields.push(ArrowField::new("__sequence", DataType::Int64, false));
    Arc::new(ArrowSchema::new(fields))
}

fn key_only_row(schema: &CdcTableSchema, key: &CdcRowKey) -> anyhow::Result<Vec<Option<RowValue>>> {
    key.validate_against_schema(schema)?;
    let mut values = vec![None; schema.columns().len()];
    for (value, column_idx) in key.values().iter().zip(schema.primary_key_indices()) {
        values[column_idx] = Some(value.clone());
    }
    Ok(values)
}

enum ArrowIpcColumnBuilder {
    Int64(Int64Builder),
    Bool(BooleanBuilder),
    Utf8(StringBuilder),
    TimestampMillis(TimestampMillisecondBuilder),
    DateDays(Date32Builder),
    Decimal128(Decimal128Builder),
    Numeric(StringBuilder),
}

impl ArrowIpcColumnBuilder {
    fn new(data_type: &ColumnType, capacity: usize) -> Self {
        match data_type {
            ColumnType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            ColumnType::Bool => Self::Bool(BooleanBuilder::with_capacity(capacity)),
            ColumnType::Utf8 => Self::Utf8(StringBuilder::with_capacity(capacity, capacity * 16)),
            ColumnType::TimestampMillis => {
                Self::TimestampMillis(TimestampMillisecondBuilder::with_capacity(capacity))
            }
            ColumnType::DateDays => Self::DateDays(Date32Builder::with_capacity(capacity)),
            ColumnType::Decimal128 { precision, scale } => Self::Decimal128(
                Decimal128Builder::with_capacity(capacity)
                    .with_data_type(DataType::Decimal128(*precision, *scale)),
            ),
            ColumnType::Numeric => {
                Self::Numeric(StringBuilder::with_capacity(capacity, capacity * 16))
            }
        }
    }

    fn append(&mut self, column: &CdcColumn, value: Option<&RowValue>) -> anyhow::Result<()> {
        match (self, column.data_type(), value) {
            (Self::Int64(builder), ColumnType::Int64, Some(RowValue::Int64(value))) => {
                builder.append_value(*value);
            }
            (Self::Bool(builder), ColumnType::Bool, Some(RowValue::Bool(value))) => {
                builder.append_value(*value);
            }
            (Self::Utf8(builder), ColumnType::Utf8, Some(RowValue::Utf8(value))) => {
                builder.append_value(value);
            }
            (
                Self::TimestampMillis(builder),
                ColumnType::TimestampMillis,
                Some(RowValue::TimestampMillis(value)),
            ) => {
                builder.append_value(*value);
            }
            (Self::DateDays(builder), ColumnType::DateDays, Some(RowValue::DateDays(value))) => {
                builder.append_value(*value);
            }
            (
                Self::Decimal128(builder),
                ColumnType::Decimal128 { .. },
                Some(RowValue::Decimal128(value)),
            ) => {
                builder.append_value(*value);
            }
            (Self::Numeric(builder), ColumnType::Numeric, Some(RowValue::Numeric(value))) => {
                builder.append_value(value);
            }
            (Self::Int64(builder), ColumnType::Int64, None) => builder.append_null(),
            (Self::Bool(builder), ColumnType::Bool, None) => builder.append_null(),
            (Self::Utf8(builder), ColumnType::Utf8, None) => builder.append_null(),
            (Self::TimestampMillis(builder), ColumnType::TimestampMillis, None) => {
                builder.append_null();
            }
            (Self::DateDays(builder), ColumnType::DateDays, None) => builder.append_null(),
            (Self::Decimal128(builder), ColumnType::Decimal128 { .. }, None) => {
                builder.append_null();
            }
            (Self::Numeric(builder), ColumnType::Numeric, None) => builder.append_null(),
            (_, _, Some(value)) => {
                return Err(anyhow!(
                    "CDC Arrow IPC value for column '{}' does not match type {:?}: {:?}",
                    column.name(),
                    column.data_type(),
                    value
                ));
            }
            _ => {
                return Err(anyhow!(
                    "CDC Arrow IPC builder for column '{}' does not match type {:?}",
                    column.name(),
                    column.data_type()
                ));
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::Bool(builder) => Arc::new(builder.finish()),
            Self::Utf8(builder) => Arc::new(builder.finish()),
            Self::TimestampMillis(builder) => Arc::new(builder.finish()),
            Self::DateDays(builder) => Arc::new(builder.finish()),
            Self::Decimal128(builder) => Arc::new(builder.finish()),
            Self::Numeric(builder) => Arc::new(builder.finish()),
        }
    }
}

struct ArrowIpcChangeBatchBuilder {
    schema: CdcTableSchema,
    arrow_schema: Arc<ArrowSchema>,
    columns: Vec<ArrowIpcColumnBuilder>,
    operations: StringBuilder,
    diffs: Int64Builder,
    sequences: Int64Builder,
    len: usize,
    capacity: usize,
    chunk_idx: usize,
}

impl ArrowIpcChangeBatchBuilder {
    fn new(schema: &CdcTableSchema, capacity: usize) -> Self {
        Self {
            schema: schema.clone(),
            arrow_schema: arrow_ipc_schema(schema),
            columns: schema
                .columns()
                .iter()
                .map(|column| ArrowIpcColumnBuilder::new(column.data_type(), capacity))
                .collect(),
            operations: StringBuilder::with_capacity(capacity, capacity * 2),
            diffs: Int64Builder::with_capacity(capacity),
            sequences: Int64Builder::with_capacity(capacity),
            len: 0,
            capacity,
            chunk_idx: 0,
        }
    }

    fn append_row(
        &mut self,
        row: &CdcRow,
        operation: &str,
        diff: i64,
        sequence: u64,
    ) -> anyhow::Result<()> {
        self.append_values(row.values(), operation, diff, sequence)
    }

    fn append_values(
        &mut self,
        values: &[Option<RowValue>],
        operation: &str,
        diff: i64,
        sequence: u64,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            values.len() == self.schema.columns().len(),
            "CDC Arrow IPC row has {} values, expected {}",
            values.len(),
            self.schema.columns().len()
        );
        for ((builder, column), value) in self
            .columns
            .iter_mut()
            .zip(self.schema.columns())
            .zip(values)
        {
            builder.append(column, value.as_ref())?;
        }
        self.operations.append_value(operation);
        self.diffs.append_value(diff);
        self.sequences
            .append_value(i64::try_from(sequence).unwrap_or(i64::MAX));
        self.len += 1;
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn is_full(&self) -> bool {
        self.len >= self.capacity
    }

    fn chunk_idx(&self) -> usize {
        self.chunk_idx
    }

    fn finish(&mut self) -> anyhow::Result<RecordBatch> {
        let mut arrays = self
            .columns
            .iter_mut()
            .map(ArrowIpcColumnBuilder::finish)
            .collect::<Vec<_>>();
        arrays.push(Arc::new(self.operations.finish()));
        arrays.push(Arc::new(self.diffs.finish()));
        arrays.push(Arc::new(self.sequences.finish()));
        let batch = RecordBatch::try_new(Arc::clone(&self.arrow_schema), arrays)
            .context("build replication Arrow IPC batch")?;
        self.columns = self
            .schema
            .columns()
            .iter()
            .map(|column| ArrowIpcColumnBuilder::new(column.data_type(), self.capacity))
            .collect();
        self.operations = StringBuilder::with_capacity(self.capacity, self.capacity * 2);
        self.diffs = Int64Builder::with_capacity(self.capacity);
        self.sequences = Int64Builder::with_capacity(self.capacity);
        self.len = 0;
        self.chunk_idx += 1;
        Ok(batch)
    }
}
