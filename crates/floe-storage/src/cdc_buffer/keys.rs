use anyhow::{Context, Result, anyhow};
use floe_cdc_core::{CdcSourcePosition, CdcTransactionId};

use crate::object_payload::{hex_component, push_length_prefixed_component};

const CDC_BUFFER_PREFIX: &[u8] = b"floe_cdc_buffer/v1/";

pub(super) fn source_frontier_key(pipeline_name: &str) -> Vec<u8> {
    let mut key = pipeline_prefix(pipeline_name);
    key.extend_from_slice(b"frontier/source");
    key
}

pub(super) fn delivery_frontier_key(pipeline_name: &str) -> Vec<u8> {
    let mut key = pipeline_prefix(pipeline_name);
    key.extend_from_slice(b"frontier/delivery");
    key
}

pub(super) fn pending_manifest_prefix(pipeline_name: &str) -> Vec<u8> {
    let mut key = pipeline_prefix(pipeline_name);
    key.extend_from_slice(b"pending/");
    key
}

pub(super) fn pending_manifest_key(pipeline_name: &str, transaction_key: &str) -> Vec<u8> {
    let mut key = pending_manifest_prefix(pipeline_name);
    key.extend_from_slice(transaction_key.as_bytes());
    key
}

pub(super) fn delivered_manifest_prefix(pipeline_name: &str) -> Vec<u8> {
    let mut key = pipeline_prefix(pipeline_name);
    key.extend_from_slice(b"delivered/");
    key
}

pub(super) fn delivered_manifest_key(
    pipeline_name: &str,
    delivered_at_unix_ms: u64,
    transaction_key: &str,
) -> Vec<u8> {
    let mut key = delivered_manifest_prefix(pipeline_name);
    key.extend_from_slice(format!("{delivered_at_unix_ms:020}/").as_bytes());
    key.extend_from_slice(transaction_key.as_bytes());
    key
}

pub(super) fn payload_object_key(pipeline_name: &str, transaction_key: &str) -> String {
    format!(
        "floe_cdc_buffer_blobs/v1/pipeline/{}/{}.bin",
        hex_component(pipeline_name.as_bytes()),
        transaction_key
    )
}

pub(super) fn payload_object_prefix(pipeline_name: &str) -> String {
    format!(
        "floe_cdc_buffer_blobs/v1/pipeline/{}/",
        hex_component(pipeline_name.as_bytes())
    )
}

fn pipeline_prefix(pipeline_name: &str) -> Vec<u8> {
    let mut key = CDC_BUFFER_PREFIX.to_vec();
    key.extend_from_slice(b"pipeline/");
    push_length_prefixed_component(&mut key, pipeline_name.as_bytes());
    key.extend_from_slice(b"/");
    key
}

pub(super) fn transaction_key(
    position: &CdcSourcePosition,
    transaction_id: Option<&CdcTransactionId>,
) -> Result<String> {
    let tx = transaction_id.map_or("none".to_string(), |tx| {
        hex_component(tx.as_str().as_bytes())
    });
    match position {
        CdcSourcePosition::Postgres {
            commit_lsn,
            event_lsn,
        } => Ok(format!(
            "pg/{:020}/{:020}/{tx}",
            parse_postgres_lsn(commit_lsn)?,
            event_lsn
                .as_deref()
                .map(parse_postgres_lsn)
                .transpose()?
                .unwrap_or(u64::MAX)
        )),
        CdcSourcePosition::Opaque { value } => {
            Ok(format!("opaque/{}/{tx}", hex_component(value.as_bytes())))
        }
    }
}

fn parse_postgres_lsn(value: &str) -> Result<u64> {
    let (high, low) = value
        .split_once('/')
        .ok_or_else(|| anyhow!("invalid Postgres LSN '{value}'"))?;
    let high = u64::from_str_radix(high, 16)
        .with_context(|| format!("invalid Postgres LSN high word '{high}'"))?;
    let low = u64::from_str_radix(low, 16)
        .with_context(|| format!("invalid Postgres LSN low word '{low}'"))?;
    Ok((high << 32) | low)
}
