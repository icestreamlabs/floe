use super::*;

pub fn catalog_db(catalog: &SlateCatalog) -> Arc<Db> {
    catalog.db()
}

pub(super) async fn scan_prefix(db: &Db, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
    let range = prefix_bounds(prefix);
    let mut iter = db
        .scan_with_options(range, &ScanOptions::default())
        .await
        .map_err(map_slate_err)?;

    let mut values = Vec::new();
    while let Some(kv) = iter.next().await.map_err(map_slate_err)? {
        values.push(kv.value.to_vec());
    }
    Ok(values)
}

pub(super) fn prefix_bounds(prefix: &[u8]) -> Range<Vec<u8>> {
    let mut end = prefix.to_vec();
    end.push(0xFF);
    prefix.to_vec()..end
}

pub(super) fn table_definition_key(name: &str) -> Vec<u8> {
    format!("{TABLE_DEF_PREFIX}{name}").into_bytes()
}

pub(super) fn source_definition_key(name: &str) -> Vec<u8> {
    format!("{SOURCE_DEF_PREFIX}{name}").into_bytes()
}

pub(super) fn source_table_key(name: &str) -> Vec<u8> {
    format!("{SOURCE_TABLE_PREFIX}{name}").into_bytes()
}

pub(super) fn mv_definition_key(name: &str) -> Vec<u8> {
    format!("{MV_DEF_PREFIX}{name}").into_bytes()
}

pub(super) fn mv_schema_key(name: &str) -> Vec<u8> {
    format!("{MV_SCHEMA_PREFIX}{name}").into_bytes()
}

pub(super) fn replication_pipeline_definition_key(name: &str) -> Vec<u8> {
    format!("{REPLICATION_PIPELINE_DEF_PREFIX}{name}").into_bytes()
}

pub(super) fn replication_pipeline_checkpoint_key(name: &str) -> Vec<u8> {
    format!("{REPLICATION_PIPELINE_CHECKPOINT_PREFIX}{name}").into_bytes()
}

pub(super) fn replication_pipeline_dlq_entry_prefix(pipeline_name: &str) -> Vec<u8> {
    format!("{REPLICATION_PIPELINE_DLQ_PREFIX}{pipeline_name}/").into_bytes()
}

pub(super) fn replication_pipeline_dlq_entry_key(pipeline_name: &str, dlq_id: &str) -> Vec<u8> {
    format!("{REPLICATION_PIPELINE_DLQ_PREFIX}{pipeline_name}/{dlq_id}").into_bytes()
}

pub(super) fn replication_pipeline_dlq_payload_object_key(
    pipeline_name: &str,
    dlq_id: &str,
) -> String {
    format!(
        "floe_cdc_dlq_blobs/v1/pipeline/{}/{}.bin",
        hex_component(pipeline_name.as_bytes()),
        dlq_id
    )
}

pub(super) fn table_row_prefix(name: &str) -> Vec<u8> {
    format!("{TABLE_DATA_PREFIX}{name}/").into_bytes()
}

pub(super) fn table_row_key(table: &TableDefinition, row: &RowValues) -> Result<Vec<u8>> {
    let pk_index = table.primary_key_index();
    let pk_value = row
        .get(pk_index)
        .cloned()
        .ok_or_else(|| anyhow!("missing value for primary key index {}", pk_index))?;
    let mut key = table_row_prefix(table.name());
    key.extend_from_slice(&encode_key_value(&pk_value)?);
    Ok(key)
}

pub(super) fn encode_key_value(value: &RowValue) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    match value {
        RowValue::Int64(v) => {
            buf.push(0x01);
            buf.extend_from_slice(&v.to_be_bytes());
        }
        RowValue::Bool(flag) => {
            buf.push(0x02);
            buf.push(if *flag { 1 } else { 0 });
        }
        RowValue::Utf8(text) => {
            buf.push(0x03);
            let bytes = text.as_bytes();
            let len =
                u32::try_from(bytes.len()).map_err(|_| anyhow!("string primary key too large"))?;
            buf.extend_from_slice(&len.to_be_bytes());
            buf.extend_from_slice(bytes);
        }
        RowValue::TimestampMillis(value) => {
            buf.push(0x04);
            buf.extend_from_slice(&value.to_be_bytes());
        }
        RowValue::DateDays(value) => {
            buf.push(0x05);
            buf.extend_from_slice(&value.to_be_bytes());
        }
        RowValue::Decimal128(value) => {
            buf.push(0x07);
            buf.extend_from_slice(&value.to_be_bytes());
        }
        RowValue::Numeric(value) => {
            buf.push(0x06);
            let bytes = value.as_bytes();
            let len =
                u32::try_from(bytes.len()).map_err(|_| anyhow!("numeric primary key too large"))?;
            buf.extend_from_slice(&len.to_be_bytes());
            buf.extend_from_slice(bytes);
        }
    }
    Ok(buf)
}

pub(super) fn map_slate_err(err: SlateError) -> anyhow::Error {
    anyhow::Error::new(err)
}
