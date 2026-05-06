use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use slatedb::Db;
use slatedb::config::ScanOptions;

const SLATEDB_NAME_ENV: &str = "FLOE_SLATEDB_NAME";
const OBJECT_STORE_ENV_FILE_ENV: &str = "FLOE_OBJECT_STORE_ENV_FILE";
const PROGRESS_EVERY_ENV: &str = "FLOE_KEYSPACE_SUMMARY_PROGRESS_EVERY";

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
    let args = parse_args()?;
    let db_name = args
        .db_name
        .or_else(|| std::env::var(SLATEDB_NAME_ENV).ok())
        .ok_or_else(|| {
            anyhow!("usage: slatedb_keyspace_summary [--keyspace <prefix>] <slatedb-name>")
        })?;
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
    let progress_every = args.progress_every;

    let ranges = if args.keyspaces.is_empty() {
        vec![vec![0x00]..vec![0xFF]]
    } else {
        args.keyspaces
            .iter()
            .map(|keyspace| prefix_range(keyspace.as_bytes()))
            .collect()
    };

    for range in ranges {
        let mut iter = db
            .scan_with_options(range, &ScanOptions::default())
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
            if progress_every > 0 && total.keys % progress_every == 0 {
                eprintln!(
                    "scanned_keys={} logical_bytes={}",
                    total.keys,
                    total.logical_bytes()
                );
            }
        }
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

#[derive(Default)]
struct Args {
    db_name: Option<String>,
    keyspaces: Vec<String>,
    progress_every: u64,
}

fn parse_args() -> Result<Args> {
    let mut parsed = Args {
        progress_every: std::env::var(PROGRESS_EVERY_ENV)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0),
        ..Args::default()
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--keyspace" {
            let keyspace = args
                .next()
                .ok_or_else(|| anyhow!("--keyspace requires a prefix"))?;
            parsed.keyspaces.push(keyspace);
        } else if arg == "--progress-every" {
            let progress_every = args
                .next()
                .ok_or_else(|| anyhow!("--progress-every requires a key count"))?;
            parsed.progress_every = progress_every
                .parse::<u64>()
                .with_context(|| format!("parse --progress-every value '{progress_every}'"))?;
        } else if parsed.db_name.is_none() {
            parsed.db_name = Some(arg);
        } else {
            return Err(anyhow!("unexpected argument '{arg}'"));
        }
    }
    Ok(parsed)
}

fn prefix_range(prefix: &[u8]) -> std::ops::Range<Vec<u8>> {
    let mut end = prefix.to_vec();
    for idx in (0..end.len()).rev() {
        if end[idx] != u8::MAX {
            end[idx] += 1;
            end.truncate(idx + 1);
            return prefix.to_vec()..end;
        }
    }
    prefix.to_vec()..vec![0xFF]
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
        || keyspace == "iba"
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
        "iba" => namespace_before_marker(
            parts,
            &["idx", "rev", "rng", "range_format", "next_segment_id"],
        ),
        _ => keyspace.to_string(),
    }
}

fn namespace_before_marker<'a>(parts: impl Iterator<Item = &'a str>, markers: &[&str]) -> String {
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
            sanitize_label(name),
            bucket.keys,
            bucket.key_bytes,
            bucket.value_bytes,
            bucket.logical_bytes()
        );
    }
}

fn sanitize_label(label: &str) -> String {
    let mut sanitized = String::with_capacity(label.len());
    for ch in label.chars() {
        if ch.is_ascii_control() {
            sanitized.push_str(&format!("\\x{:02x}", ch as u32));
        } else {
            sanitized.push(ch);
        }
    }
    sanitized
}
