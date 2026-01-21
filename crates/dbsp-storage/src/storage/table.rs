use std::ops::Range;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use slatedb::config::{ScanOptions, WriteOptions};
use slatedb::{Db, WriteBatch};

use super::map_slate_err;

pub fn prefix_bounds(prefix: &[u8]) -> Range<Vec<u8>> {
    let mut end = prefix.to_vec();
    end.push(0xFF);
    prefix.to_vec()..end
}

#[async_trait]
pub trait KeyValueTable: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    async fn write_batch(&self, batch: WriteBatch) -> Result<()>;
    async fn scan_range(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write_batch(batch).await
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write_batch(batch).await
    }

    async fn scan_prefix(
        &self,
        prefix: &[u8],
        options: &ScanOptions,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_range(prefix_bounds(prefix), options).await
    }
}

pub struct SlateTable {
    db: Arc<Db>,
}

impl SlateTable {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }

    fn write_options() -> WriteOptions {
        WriteOptions {
            await_durable: false,
        }
    }
}

#[async_trait]
impl KeyValueTable for SlateTable {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db
            .get(key)
            .await
            .map(|opt| opt.map(|value| value.to_vec()))
            .map_err(map_slate_err)
    }

    async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.db
            .write_with_options(batch, &Self::write_options())
            .await
            .map_err(map_slate_err)
    }

    async fn scan_range(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self
            .db
            .scan_with_options(range, options)
            .await
            .map_err(map_slate_err)?;

        let mut entries = Vec::new();
        while let Some(kv) = iter.next().await.map_err(map_slate_err)? {
            entries.push((kv.key.to_vec(), kv.value.to_vec()));
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::KeyValueTable;
    use crate::storage::keyspace::namespace_prefix;
    use crate::storage::timestamps;
    use object_store::memory::InMemory;

    async fn build_table(name: &str) -> Arc<dyn KeyValueTable> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(name, store).await.expect("open SlateDB"));
        Arc::new(SlateTable::new(db))
    }

    #[tokio::test]
    async fn scan_prefix_returns_chronological_order() {
        let table = build_table("storage-ordering").await;

        let mut base = namespace_prefix(crate::storage::keyspace::prefix::STREAM, "ordering");
        base.extend_from_slice(b"data/");

        let mut timestamps_to_insert = vec![5_i64, 1, 3, 2, 4];
        for &ts in &timestamps_to_insert {
            let key = timestamps::append(&base, ts).expect("encode key");
            let value = ts.to_be_bytes();
            table.put(&key, &value).await.expect("write entry");
        }

        let entries = table
            .scan_prefix(&base, &ScanOptions::default())
            .await
            .expect("scan entries");

        let observed: Vec<i64> = entries
            .into_iter()
            .map(|(key, value)| {
                let ts = timestamps::extract(&base, &key).expect("decode timestamp");
                let stored = i64::from_be_bytes(value.try_into().expect("value width"));
                assert_eq!(ts, stored);
                ts
            })
            .collect();

        timestamps_to_insert.sort();
        assert_eq!(observed, timestamps_to_insert);
    }
}
