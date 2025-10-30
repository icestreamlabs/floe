use std::ops::Range;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use slatedb::config::{ScanOptions, WriteOptions};
use slatedb::{Db, WriteBatch};

use super::map_slate_err;

#[async_trait]
pub trait KeyValueTable: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    async fn write_batch(&self, batch: WriteBatch) -> Result<()>;
    async fn scan_range(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
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
