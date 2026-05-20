use anyhow::{Context, Result, anyhow, ensure};
use floe_cdc_core::ChangeBatch;

use super::CdcBufferRecord;

pub(super) const CDC_BUFFER_PAYLOAD_MAGIC_V1: &[u8; 8] = b"FCDCBUF1";
const CDC_BUFFER_PAYLOAD_MAGIC: &[u8; 8] = b"FCDCBUF2";
const CDC_BUFFER_CHANGE_BATCHES_MAGIC: &[u8; 8] = b"FCDCBCH1";
const CDC_BUFFER_NONE_LEN: u64 = u64::MAX;

pub(super) fn encode_payload_records(records: &[CdcBufferRecord]) -> Result<Vec<u8>> {
    let record_count =
        u64::try_from(records.len()).context("CDC buffer record count exceeds u64")?;
    let mut out = Vec::with_capacity(
        CDC_BUFFER_PAYLOAD_MAGIC.len()
            + std::mem::size_of::<u64>()
            + records
                .iter()
                .map(|record| {
                    std::mem::size_of::<u64>() * 2
                        + record.key().map_or(0, |key| key.len())
                        + record.value().map_or(0, |value| value.len())
                        + std::mem::size_of::<u64>()
                        + record
                            .headers()
                            .iter()
                            .map(|header| {
                                std::mem::size_of::<u64>() * 2
                                    + header.key().len()
                                    + header.value().len()
                            })
                            .sum::<usize>()
                })
                .sum::<usize>(),
    );
    out.extend_from_slice(CDC_BUFFER_PAYLOAD_MAGIC);
    out.extend_from_slice(&record_count.to_be_bytes());
    for record in records {
        encode_optional_bytes(&mut out, record.key())?;
        encode_optional_bytes(&mut out, record.value())?;
        let header_count = u64::try_from(record.headers().len())
            .context("CDC buffer record header count exceeds u64")?;
        out.extend_from_slice(&header_count.to_be_bytes());
        for header in record.headers() {
            encode_required_bytes(&mut out, header.key().as_bytes())?;
            encode_required_bytes(&mut out, header.value())?;
        }
    }
    Ok(out)
}

pub fn encode_cdc_buffer_records_payload(records: &[CdcBufferRecord]) -> Result<Vec<u8>> {
    encode_payload_records(records)
}

pub(super) fn encode_payload_change_batches(change_batches: &[ChangeBatch]) -> Result<Vec<u8>> {
    ensure!(
        !change_batches.is_empty(),
        "CDC buffer change batch payload cannot be empty"
    );
    let payload = serde_json::to_vec(change_batches).context("encode CDC buffer change batches")?;
    let mut out = Vec::with_capacity(CDC_BUFFER_CHANGE_BATCHES_MAGIC.len() + payload.len());
    out.extend_from_slice(CDC_BUFFER_CHANGE_BATCHES_MAGIC);
    out.extend_from_slice(&payload);
    Ok(out)
}

pub(super) fn encode_optional_bytes(out: &mut Vec<u8>, value: Option<&[u8]>) -> Result<()> {
    let Some(value) = value else {
        out.extend_from_slice(&CDC_BUFFER_NONE_LEN.to_be_bytes());
        return Ok(());
    };
    let len = u64::try_from(value.len()).context("CDC buffer payload field exceeds u64")?;
    ensure!(
        len != CDC_BUFFER_NONE_LEN,
        "CDC buffer payload field length uses reserved sentinel"
    );
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value);
    Ok(())
}

fn encode_required_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    encode_optional_bytes(out, Some(value))
}

pub(super) fn decode_payload_records(payload: &[u8]) -> Result<Vec<CdcBufferRecord>> {
    ensure!(
        payload.len() >= CDC_BUFFER_PAYLOAD_MAGIC.len() + std::mem::size_of::<u64>(),
        "CDC buffer payload blob is too short"
    );
    let magic = &payload[..CDC_BUFFER_PAYLOAD_MAGIC.len()];
    ensure!(
        magic == CDC_BUFFER_PAYLOAD_MAGIC || magic == CDC_BUFFER_PAYLOAD_MAGIC_V1,
        "CDC buffer payload blob has invalid magic"
    );
    let has_headers = magic == CDC_BUFFER_PAYLOAD_MAGIC;
    let mut cursor = CDC_BUFFER_PAYLOAD_MAGIC.len();
    let record_count = read_u64(payload, &mut cursor)?;
    let record_count =
        usize::try_from(record_count).context("CDC buffer record count exceeds usize")?;
    let mut records = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        let key = decode_optional_bytes(payload, &mut cursor)?;
        let value = decode_optional_bytes(payload, &mut cursor)?;
        let mut record = CdcBufferRecord::new(key, value);
        if has_headers {
            let header_count = read_u64(payload, &mut cursor)?;
            let header_count = usize::try_from(header_count)
                .context("CDC buffer record header count exceeds usize")?;
            for _ in 0..header_count {
                let key = decode_required_bytes(payload, &mut cursor)?;
                let key = String::from_utf8(key)
                    .context("CDC buffer record header key is not valid UTF-8")?;
                let value = decode_required_bytes(payload, &mut cursor)?;
                record = record.with_header(key, value);
            }
        }
        records.push(record);
    }
    ensure!(
        cursor == payload.len(),
        "CDC buffer payload blob has trailing bytes"
    );
    Ok(records)
}

pub fn decode_cdc_buffer_records_payload(payload: &[u8]) -> Result<Vec<CdcBufferRecord>> {
    decode_payload_records(payload)
}

pub(super) fn decode_payload_change_batches(payload: &[u8]) -> Result<Vec<ChangeBatch>> {
    ensure!(
        payload.len() >= CDC_BUFFER_CHANGE_BATCHES_MAGIC.len(),
        "CDC buffer change batch payload blob is too short"
    );
    ensure!(
        &payload[..CDC_BUFFER_CHANGE_BATCHES_MAGIC.len()] == CDC_BUFFER_CHANGE_BATCHES_MAGIC,
        "CDC buffer change batch payload blob has invalid magic"
    );
    let batches = serde_json::from_slice::<Vec<ChangeBatch>>(
        &payload[CDC_BUFFER_CHANGE_BATCHES_MAGIC.len()..],
    )
    .context("decode CDC buffer change batches")?;
    ensure!(
        !batches.is_empty(),
        "CDC buffer change batch payload cannot be empty"
    );
    Ok(batches)
}

fn decode_optional_bytes(payload: &[u8], cursor: &mut usize) -> Result<Option<Vec<u8>>> {
    let len = read_u64(payload, cursor)?;
    if len == CDC_BUFFER_NONE_LEN {
        return Ok(None);
    }
    let len = usize::try_from(len).context("CDC buffer payload field length exceeds usize")?;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| anyhow!("CDC buffer payload field length overflow"))?;
    ensure!(
        end <= payload.len(),
        "CDC buffer payload field extends past blob end"
    );
    let value = payload[*cursor..end].to_vec();
    *cursor = end;
    Ok(Some(value))
}

fn decode_required_bytes(payload: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let value = decode_optional_bytes(payload, cursor)?;
    value.ok_or_else(|| anyhow!("CDC buffer payload field unexpectedly used null sentinel"))
}

fn read_u64(payload: &[u8], cursor: &mut usize) -> Result<u64> {
    let end = cursor
        .checked_add(std::mem::size_of::<u64>())
        .ok_or_else(|| anyhow!("CDC buffer payload cursor overflow"))?;
    ensure!(end <= payload.len(), "CDC buffer payload ended early");
    let mut bytes = [0_u8; std::mem::size_of::<u64>()];
    bytes.copy_from_slice(&payload[*cursor..end]);
    *cursor = end;
    Ok(u64::from_be_bytes(bytes))
}
