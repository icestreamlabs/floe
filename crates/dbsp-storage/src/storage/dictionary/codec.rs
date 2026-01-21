use anyhow::{Result, anyhow};
use snap::raw::{Decoder, Encoder};

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
    let mut encoder = Encoder::new();
    encoder
        .compress_vec(bytes)
        .map_err(|err| anyhow!("failed to compress dictionary value: {err}"))
}

pub(super) fn decompress_value(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = Decoder::new();
    decoder
        .decompress_vec(bytes)
        .map_err(|err| anyhow!("failed to decompress dictionary value: {err}"))
}
