use anyhow::{Context, Result};
use floe_cdc_core::{CdcRowKey, CdcSourceId, CdcTableId};

const CDC_PREFIX: &[u8] = b"floe_cdc/v1/";

pub(crate) fn source_metadata_prefix() -> Vec<u8> {
    let mut key = CDC_PREFIX.to_vec();
    key.extend_from_slice(b"meta/source/");
    key
}

pub(crate) fn source_metadata_key(source_id: &CdcSourceId) -> Vec<u8> {
    let mut key = source_metadata_prefix();
    push_component(&mut key, source_id.as_str().as_bytes());
    key
}

pub(crate) fn table_metadata_key(table_id: &CdcTableId) -> Vec<u8> {
    let mut key = CDC_PREFIX.to_vec();
    key.extend_from_slice(b"meta/table/");
    push_component(&mut key, table_id.as_str().as_bytes());
    key
}

pub(crate) fn source_table_index_prefix(source_id: &CdcSourceId) -> Vec<u8> {
    let mut key = CDC_PREFIX.to_vec();
    key.extend_from_slice(b"meta/source_table/");
    push_component(&mut key, source_id.as_str().as_bytes());
    key
}

pub(crate) fn source_table_index_key(source_id: &CdcSourceId, table_id: &CdcTableId) -> Vec<u8> {
    let mut key = source_table_index_prefix(source_id);
    push_component(&mut key, table_id.as_str().as_bytes());
    key
}

pub(crate) fn checkpoint_key(source_id: &CdcSourceId) -> Vec<u8> {
    let mut key = CDC_PREFIX.to_vec();
    key.extend_from_slice(b"checkpoint/");
    push_component(&mut key, source_id.as_str().as_bytes());
    key
}

pub(crate) fn row_key_bytes(table_id: &CdcTableId, row_key: &CdcRowKey) -> Result<Vec<u8>> {
    let mut key = CDC_PREFIX.to_vec();
    key.extend_from_slice(b"row/");
    push_component(&mut key, table_id.as_str().as_bytes());
    push_component(
        &mut key,
        &serde_json::to_vec(row_key).context("encode CDC row key")?,
    );
    Ok(key)
}

fn push_component(out: &mut Vec<u8>, component: &[u8]) {
    let len = u32::try_from(component.len()).expect("CDC key component length exceeds u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(component);
}
