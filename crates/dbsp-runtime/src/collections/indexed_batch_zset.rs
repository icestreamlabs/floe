use anyhow::{Result, anyhow};

/// Encode keys into an order-preserving byte representation for range scans.
pub trait RangeKey {
    fn encode_range_key(&self) -> Vec<u8>;
    fn encoded_len(encoded: &[u8]) -> Result<usize>;
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct OrderedBytes(pub Vec<u8>);

impl OrderedBytes {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for OrderedBytes {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<&[u8]> for OrderedBytes {
    fn from(value: &[u8]) -> Self {
        Self(value.to_vec())
    }
}

impl From<&str> for OrderedBytes {
    fn from(value: &str) -> Self {
        Self(value.as_bytes().to_vec())
    }
}

fn encode_memcomparable(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 2);
    for &b in bytes {
        if b == 0 {
            out.push(0);
            out.push(0xFF);
        } else {
            out.push(b);
        }
    }
    out.push(0);
    out.push(0);
    out
}

fn memcomparable_len(encoded: &[u8]) -> Result<usize> {
    let mut idx = 0;
    while idx + 1 < encoded.len() {
        if encoded[idx] != 0 {
            idx += 1;
            continue;
        }
        match encoded[idx + 1] {
            0xFF => idx += 2,
            0x00 => return Ok(idx + 2),
            other => return Err(anyhow!("invalid memcomparable escape byte: {other:#04x}")),
        }
    }
    Err(anyhow!("truncated memcomparable encoding"))
}

impl RangeKey for i64 {
    fn encode_range_key(&self) -> Vec<u8> {
        let shifted = (*self as u64) ^ 0x8000_0000_0000_0000;
        shifted.to_be_bytes().to_vec()
    }

    fn encoded_len(_encoded: &[u8]) -> Result<usize> {
        Ok(8)
    }
}

impl RangeKey for u64 {
    fn encode_range_key(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }

    fn encoded_len(_encoded: &[u8]) -> Result<usize> {
        Ok(8)
    }
}

impl RangeKey for i32 {
    fn encode_range_key(&self) -> Vec<u8> {
        let shifted = (*self as u32) ^ 0x8000_0000;
        shifted.to_be_bytes().to_vec()
    }

    fn encoded_len(_encoded: &[u8]) -> Result<usize> {
        Ok(4)
    }
}

impl RangeKey for u32 {
    fn encode_range_key(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }

    fn encoded_len(_encoded: &[u8]) -> Result<usize> {
        Ok(4)
    }
}

impl RangeKey for OrderedBytes {
    fn encode_range_key(&self) -> Vec<u8> {
        encode_memcomparable(self.as_bytes())
    }

    fn encoded_len(encoded: &[u8]) -> Result<usize> {
        memcomparable_len(encoded)
    }
}

impl RangeKey for String {
    fn encode_range_key(&self) -> Vec<u8> {
        encode_memcomparable(self.as_bytes())
    }

    fn encoded_len(encoded: &[u8]) -> Result<usize> {
        memcomparable_len(encoded)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ApplyDeltaMetrics {
    pub input_records: usize,
    pub non_zero_input_records: usize,
    pub coalesced_records: usize,
    pub persisted_records: usize,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LookupMetrics {
    pub lookup_keys: usize,
    pub returned_rows: usize,
    pub index_segments_examined: usize,
    pub index_postings_examined: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
}

impl LookupMetrics {
    pub fn add_assign(&mut self, other: Self) {
        self.lookup_keys = self.lookup_keys.saturating_add(other.lookup_keys);
        self.returned_rows = self.returned_rows.saturating_add(other.returned_rows);
        self.index_segments_examined = self
            .index_segments_examined
            .saturating_add(other.index_segments_examined);
        self.index_postings_examined = self
            .index_postings_examined
            .saturating_add(other.index_postings_examined);
        self.cache_hits = self.cache_hits.saturating_add(other.cache_hits);
        self.cache_misses = self.cache_misses.saturating_add(other.cache_misses);
    }
}

#[cfg(test)]
mod tests {
    use super::{OrderedBytes, RangeKey};

    #[test]
    fn ordered_bytes_range_encoding_is_memcomparable() {
        let mut keys = vec![
            OrderedBytes::from("b"),
            OrderedBytes::from("aa"),
            OrderedBytes::from("c"),
            OrderedBytes::from("a\0b"),
        ];
        keys.sort_by_key(|k| k.encode_range_key());
        assert_eq!(
            keys,
            vec![
                OrderedBytes::from("a\0b"),
                OrderedBytes::from("aa"),
                OrderedBytes::from("b"),
                OrderedBytes::from("c"),
            ]
        );
    }
}
