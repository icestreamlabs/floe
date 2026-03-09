use anyhow::{Result, anyhow};
use snap::raw::{Decoder, Encoder};

const VALUE_CODEC_RAW: u8 = 0x00;
const VALUE_CODEC_SNAP: u8 = 0x01;
const RAW_THRESHOLD_BYTES: usize = 256;

pub(super) fn encode_id(id: u64) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

pub(super) fn decode_id(bytes: &[u8]) -> Result<u64> {
    if bytes.len() != 8 {
        return Err(anyhow!("expected 8 bytes for dictionary id"));
    }
    let mut array = [0u8; 8];
    array.copy_from_slice(bytes);
    Ok(u64::from_be_bytes(array))
}

pub(super) fn compress_value(bytes: &[u8]) -> Result<Vec<u8>> {
    if bytes.len() <= RAW_THRESHOLD_BYTES {
        let mut out = Vec::with_capacity(1 + bytes.len());
        out.push(VALUE_CODEC_RAW);
        out.extend_from_slice(bytes);
        return Ok(out);
    }

    let mut encoder = Encoder::new();
    let compressed = encoder
        .compress_vec(bytes)
        .map_err(|err| anyhow!("failed to compress dictionary value: {err}"))?;

    // Keep incompressible payloads raw to avoid unnecessary CPU work on read.
    if compressed.len() + 1 >= bytes.len() {
        let mut out = Vec::with_capacity(1 + bytes.len());
        out.push(VALUE_CODEC_RAW);
        out.extend_from_slice(bytes);
        return Ok(out);
    }

    let mut out = Vec::with_capacity(1 + compressed.len());
    out.push(VALUE_CODEC_SNAP);
    out.extend_from_slice(&compressed);
    Ok(out)
}

pub(super) fn decompress_value(bytes: &[u8]) -> Result<Vec<u8>> {
    if let Some((codec, payload)) = bytes.split_first() {
        match *codec {
            VALUE_CODEC_RAW => return Ok(payload.to_vec()),
            VALUE_CODEC_SNAP => {
                let mut decoder = Decoder::new();
                return decoder
                    .decompress_vec(payload)
                    .map_err(|err| anyhow!("failed to decompress dictionary value: {err}"));
            }
            _ => {
                // Backward compatibility for values written before codec tagging.
            }
        }
    }

    let mut decoder = Decoder::new();
    decoder
        .decompress_vec(bytes)
        .map_err(|err| anyhow!("failed to decompress dictionary value: {err}"))
}
