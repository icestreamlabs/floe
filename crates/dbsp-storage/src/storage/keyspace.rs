/// Canonical keyspace prefixes used across DBSP storage layouts.
pub mod prefix {
    pub const STREAM: &str = "stream/";
    pub const ZSET: &str = "zset/";
    pub const INDEX: &str = "index/";
    pub const DICT: &str = "dict/";
    pub const SEGMENT: &str = "segment/";
    pub const MANIFEST: &str = "manifest/";
    pub const DATA: &str = "data/";
    pub const INDEX_LAYER: &str = "index/";
    pub const GC: &str = "gc/";
    pub const TOMBSTONE: &str = "tombstone/";
    pub const PIN: &str = "pin/";
    pub const INTENT: &str = "intent";
}

/// Builds a namespaced prefix by combining the base keyspace prefix with the namespace.
pub fn namespace_prefix(base: &str, namespace: &str) -> Vec<u8> {
    let mut bytes = base.as_bytes().to_vec();
    bytes.extend_from_slice(namespace.as_bytes());
    bytes.push(b'/');
    bytes
}

/// Builds a namespaced prefix and appends one or more scope path segments.
pub fn scoped_prefix(base: &str, namespace: &str, scope: &[&str]) -> Vec<u8> {
    let mut bytes = namespace_prefix(base, namespace);
    for segment in scope {
        bytes.extend_from_slice(segment.as_bytes());
    }
    bytes
}

pub fn segment_data_prefix(namespace: &str) -> Vec<u8> {
    scoped_prefix(prefix::ZSET, namespace, &[prefix::SEGMENT, prefix::DATA])
}

pub fn data_manifest_prefix(namespace: &str) -> Vec<u8> {
    scoped_prefix(prefix::ZSET, namespace, &[prefix::MANIFEST, prefix::DATA])
}

pub fn index_manifest_prefix(namespace: &str) -> Vec<u8> {
    scoped_prefix(
        prefix::INDEX,
        namespace,
        &[prefix::MANIFEST, prefix::INDEX_LAYER],
    )
}

pub fn gc_tombstone_prefix(namespace: &str) -> Vec<u8> {
    scoped_prefix(prefix::GC, namespace, &[prefix::TOMBSTONE])
}

pub fn gc_pin_prefix(namespace: &str) -> Vec<u8> {
    scoped_prefix(prefix::GC, namespace, &[prefix::PIN])
}

pub fn intent_key(prefix: &[u8]) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(prefix::INTENT.as_bytes());
    key
}

pub fn key_with_u64(prefix: &[u8], value: u64) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(&value.to_be_bytes());
    key
}

pub fn parse_u64_key_suffix(prefix: &[u8], key: &[u8]) -> Option<u64> {
    if !key.starts_with(prefix) {
        return None;
    }
    let suffix = &key[prefix.len()..];
    if suffix.len() != std::mem::size_of::<u64>() {
        return None;
    }
    let bytes: [u8; 8] = suffix.try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_segment_index_and_gc_prefixes() {
        assert_eq!(
            segment_data_prefix("orders"),
            b"zset/orders/segment/data/".to_vec()
        );
        assert_eq!(
            index_manifest_prefix("orders"),
            b"index/orders/manifest/index/".to_vec()
        );
        assert_eq!(
            gc_tombstone_prefix("orders"),
            b"gc/orders/tombstone/".to_vec()
        );
        assert_eq!(gc_pin_prefix("orders"), b"gc/orders/pin/".to_vec());
    }

    #[test]
    fn parses_fixed_width_u64_suffix() {
        let prefix = b"zset/orders/segment/data/";
        let key = key_with_u64(prefix, 42);
        assert_eq!(parse_u64_key_suffix(prefix, &key), Some(42));
    }

    #[test]
    fn rejects_non_matching_u64_suffix() {
        let prefix = b"zset/orders/segment/data/";
        let mut wrong_prefix = prefix.to_vec();
        wrong_prefix[0] = b'x';
        assert_eq!(
            parse_u64_key_suffix(&wrong_prefix, &key_with_u64(prefix, 42)),
            None
        );
        assert_eq!(parse_u64_key_suffix(prefix, prefix), None);
    }
}
