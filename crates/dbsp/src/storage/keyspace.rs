/// Canonical keyspace prefixes used across DBSP storage layouts.
#[allow(dead_code)]
pub mod prefix {
    pub const STREAM: &str = "stream/";
    pub const ZSET: &str = "zset/";
    pub const INDEX: &str = "index/";
    pub const DICT: &str = "dict/";
}

/// Builds a namespaced prefix by combining the base keyspace prefix with the namespace.
pub fn namespace_prefix(base: &str, namespace: &str) -> Vec<u8> {
    let mut bytes = base.as_bytes().to_vec();
    bytes.extend_from_slice(namespace.as_bytes());
    bytes.push(b'/');
    bytes
}
