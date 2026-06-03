use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
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
    async fn get_bytes(&self, key: &[u8]) -> Result<Option<Bytes>>;
    async fn write_batch(&self, batch: WriteBatch) -> Result<()>;
    async fn scan_range_bytes(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
    ) -> Result<Vec<(Bytes, Bytes)>>;

    async fn scan_range_bytes_until(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
        should_continue_after_entry: &mut (
                 dyn for<'a, 'b> FnMut(&'a [u8], &'b [u8]) -> Result<bool> + Send
             ),
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let entries = self.scan_range_bytes(range, options).await?;
        let mut output = Vec::new();
        for (key, value) in entries {
            let should_continue = should_continue_after_entry(key.as_ref(), value.as_ref())?;
            output.push((key, value));
            if !should_continue {
                break;
            }
        }
        Ok(output)
    }

    async fn scan_range_bytes_for_each(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
        visit_entry: &mut (dyn for<'a, 'b> FnMut(&'a [u8], &'b [u8]) -> Result<()> + Send),
    ) -> Result<()> {
        let entries = self.scan_range_bytes(range, options).await?;
        for (key, value) in entries {
            visit_entry(key.as_ref(), value.as_ref())?;
        }
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_bytes(key)
            .await
            .map(|opt| opt.map(|value| value.to_vec()))
    }

    async fn scan_range(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_range_bytes(range, options).await.map(|entries| {
            entries
                .into_iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec()))
                .collect()
        })
    }

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

    async fn scan_prefix_bytes(
        &self,
        prefix: &[u8],
        options: &ScanOptions,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        self.scan_range_bytes(prefix_bounds(prefix), options).await
    }
}

pub struct SlateTable {
    db: Arc<Db>,
    await_durable: bool,
}

static DEFAULT_AWAIT_DURABLE: AtomicBool = AtomicBool::new(false);

impl SlateTable {
    pub fn new(db: Arc<Db>) -> Self {
        Self {
            db,
            await_durable: Self::default_await_durable(),
        }
    }

    pub fn with_await_durable(db: Arc<Db>, await_durable: bool) -> Self {
        Self { db, await_durable }
    }

    pub fn set_default_await_durable(await_durable: bool) {
        DEFAULT_AWAIT_DURABLE.store(await_durable, Ordering::Relaxed);
    }

    pub fn default_await_durable() -> bool {
        DEFAULT_AWAIT_DURABLE.load(Ordering::Relaxed)
    }

    pub fn await_durable_enabled(&self) -> bool {
        self.await_durable
    }

    fn write_options(&self) -> WriteOptions {
        WriteOptions {
            await_durable: self.await_durable,
            ..WriteOptions::default()
        }
    }
}

#[async_trait]
impl KeyValueTable for SlateTable {
    async fn get_bytes(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.db.get(key).await.map_err(map_slate_err)
    }

    async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.db
            .write_with_options(batch, &self.write_options())
            .await
            .map(|_| ())
            .map_err(map_slate_err)
    }

    async fn scan_range_bytes(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let mut iter = self
            .db
            .scan_with_options(range, options)
            .await
            .map_err(map_slate_err)?;

        let mut entries = Vec::new();
        while let Some(kv) = iter.next().await.map_err(map_slate_err)? {
            entries.push((kv.key, kv.value));
        }
        Ok(entries)
    }

    async fn scan_range_bytes_until(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
        should_continue_after_entry: &mut (
                 dyn for<'a, 'b> FnMut(&'a [u8], &'b [u8]) -> Result<bool> + Send
             ),
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let mut iter = self
            .db
            .scan_with_options(range, options)
            .await
            .map_err(map_slate_err)?;

        let mut entries = Vec::new();
        while let Some(kv) = iter.next().await.map_err(map_slate_err)? {
            let should_continue = should_continue_after_entry(kv.key.as_ref(), kv.value.as_ref())?;
            entries.push((kv.key, kv.value));
            if !should_continue {
                break;
            }
        }
        Ok(entries)
    }

    async fn scan_range_bytes_for_each(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
        visit_entry: &mut (dyn for<'a, 'b> FnMut(&'a [u8], &'b [u8]) -> Result<()> + Send),
    ) -> Result<()> {
        let mut iter = self
            .db
            .scan_with_options(range, options)
            .await
            .map_err(map_slate_err)?;

        while let Some(kv) = iter.next().await.map_err(map_slate_err)? {
            visit_entry(kv.key.as_ref(), kv.value.as_ref())?;
        }
        Ok(())
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

    #[tokio::test]
    async fn explicit_await_durable_mode_is_configurable() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(
            Db::open("storage-durable-mode", store)
                .await
                .expect("open SlateDB"),
        );

        let durable = SlateTable::with_await_durable(db.clone(), true);
        assert!(durable.await_durable_enabled());
        durable
            .put(b"durable/key", b"value")
            .await
            .expect("durable write");

        let buffered = SlateTable::with_await_durable(db, false);
        assert!(!buffered.await_durable_enabled());
        buffered
            .put(b"buffered/key", b"value")
            .await
            .expect("buffered write");
    }
}
