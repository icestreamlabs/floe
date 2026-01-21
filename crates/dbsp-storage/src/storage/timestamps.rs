use anyhow::{Result, anyhow};

/// Appends a big-endian `i64` timestamp to the provided prefix, returning the new key.
pub fn append(prefix: &[u8], timestamp: i64) -> Result<Vec<u8>> {
    if timestamp < 0 {
        return Err(anyhow!("timestamp cannot be negative"));
    }

    let mut key = Vec::with_capacity(prefix.len() + 8);
    key.extend_from_slice(prefix);
    key.extend_from_slice(&timestamp.to_be_bytes());
    Ok(key)
}

/// Extracts the timestamp suffix from a key previously built with [`append`].
pub fn extract(prefix: &[u8], key: &[u8]) -> Result<i64> {
    if key.len() != prefix.len() + 8 || !key.starts_with(prefix) {
        return Err(anyhow!("invalid key while decoding timestamp"));
    }

    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&key[prefix.len()..]);
    Ok(i64::from_be_bytes(bytes))
}
