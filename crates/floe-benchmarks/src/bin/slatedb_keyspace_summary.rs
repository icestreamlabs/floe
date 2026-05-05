use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use slatedb::Db;
use slatedb::config::ScanOptions;

const SLATEDB_NAME_ENV: &str = "FLOE_SLATEDB_NAME";
const OBJECT_STORE_ENV_FILE_ENV: &str = "FLOE_OBJECT_STORE_ENV_FILE";

#[derive(Default)]
struct Bucket {
    keys: u64,
    key_bytes: u64,
    value_bytes: u64,
}

impl Bucket {
    fn add(&mut self, key_len: usize, value_len: usize) {
        self.keys += 1;
        self.key_bytes += key_len as u64;
        self.value_bytes += value_len as u64;
    }

    fn logical_bytes(&self) -> u64 {
        self.key_bytes + self.value_bytes
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let db_name = std::env::args()
        .nth(1)
        .or_else(|| std::env::var(SLATEDB_NAME_ENV).ok())
        .ok_or_else(|| anyhow!("usage: slatedb_keyspace_summary <slatedb-name>"))?;
    let env_file = std::env::var(OBJECT_STORE_ENV_FILE_ENV).ok();
    let object_store = slatedb::admin::load_object_store_from_env(env_file)
        .map_err(|err| anyhow!("{err}"))
        .context("load SlateDB object store from environment")?;
    let db = Arc::new(
        Db::open(db_name.as_str(), object_store)
            .await
            .with_context(|| format!("open SlateDB prefix '{db_name}'"))?,
    );

    let mut by_class = BTreeMap::<String, Bucket>::new();
    let mut by_namespace = BTreeMap::<String, Bucket>::new();
    let mut by_keyspace = BTreeMap::<String, Bucket>::new();
    let mut total = Bucket::default();

    let mut iter = db
        .scan_with_options(vec![0x00]..vec![0xFF], &ScanOptions::default())
        .await
        .context("scan SlateDB keyspace")?;
    while let Some(kv) = iter.next().await.context("scan next SlateDB key")? {
        let key = String::from_utf8_lossy(kv.key.as_ref());
        let class = classify_key(&key);
        let namespace = namespace_summary_key(&key);
        let keyspace = key.split('/').next().unwrap_or("<empty>").to_string();
        let key_len = kv.key.len();
        let value_len = kv.value.len();

        total.add(key_len, value_len);
        by_class.entry(class).or_default().add(key_len, value_len);
        by_namespace
            .entry(namespace)
            .or_default()
            .add(key_len, value_len);
        by_keyspace
            .entry(keyspace)
            .or_default()
            .add(key_len, value_len);
    }

    print_table("class", &by_class);
    print_table("namespace", &by_namespace);
    print_table("keyspace", &by_keyspace);
    println!(
        "total\t{}\t{}\t{}\t{}",
        total.keys,
        total.key_bytes,
        total.value_bytes,
        total.logical_bytes()
    );

    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), db.close()).await;
    Ok(())
}

fn classify_key(key: &str) -> String {
    let namespace = namespace_summary_key(key);
    let keyspace = key.split('/').next().unwrap_or("<empty>");
    if namespace.starts_with("mv/") {
        "materialized_view".to_string()
    } else if namespace == "floe_runtime" {
        "checkpoint".to_string()
    } else if namespace.starts_with("source_journal") {
        "source_journal".to_string()
    } else if namespace.starts_with("codec/") {
        "codec".to_string()
    } else if namespace.contains("transient")
        || namespace.contains("join_pipeline")
        || namespace.contains("source_topn")
        || namespace.contains("source_aggregate")
        || namespace.contains("source_window")
        || namespace == "floe"
        || keyspace == "indexed_batch_arrow"
    {
        "operator_state".to_string()
    } else {
        "other".to_string()
    }
}

fn namespace_summary_key(key: &str) -> String {
    let mut parts = key.split('/');
    let Some(keyspace) = parts.next() else {
        return "<empty>".to_string();
    };
    match keyspace {
        "dict" => namespace_before_marker(parts, &["k2id", "id2k"]),
        "zset" => namespace_before_marker(parts, &["segment", "manifest"]),
        "index" => namespace_before_marker(parts, &["segment", "manifest"]),
        "gc" => namespace_before_marker(parts, &["tombstone", "pin"]),
        "stream" => namespace_before_marker(parts, &["data", "metadata"]),
        _ => keyspace.to_string(),
    }
}

fn namespace_before_marker<'a>(
    parts: impl Iterator<Item = &'a str>,
    markers: &[&str],
) -> String {
    let mut namespace = Vec::new();
    for part in parts {
        if markers.contains(&part) {
            break;
        }
        namespace.push(part);
    }
    if namespace.is_empty() {
        "<none>".to_string()
    } else {
        namespace.join("/")
    }
}

fn print_table(label: &str, buckets: &BTreeMap<String, Bucket>) {
    println!("{label}\tkeys\tkey_bytes\tvalue_bytes\tlogical_bytes");
    for (name, bucket) in buckets {
        println!(
            "{}\t{}\t{}\t{}\t{}",
            name,
            bucket.keys,
            bucket.key_bytes,
            bucket.value_bytes,
            bucket.logical_bytes()
        );
    }
}
