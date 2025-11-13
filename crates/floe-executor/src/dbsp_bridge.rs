use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use anyhow::Result;
use dbsp::handles::{ZSetHandle, ZSetHandleView};
use dbsp::storage::dictionary::Dictionary;
use dbsp::storage::{KeyValueTable, SlateTable};
use dbsp::{Stream, StreamRetention, ZSetStream};
use slatedb::Db;

use crate::namespaces;

/// Shared bridge that provisions DBSP-backed views for materialization.
pub struct DbspBridge {
    table: Arc<dyn KeyValueTable>,
    dictionaries: HashMap<String, Arc<Dictionary<Vec<u8>>>>,
}

impl DbspBridge {
    pub async fn new(db: Arc<Db>) -> Result<Self> {
        Ok(Self {
            table: Arc::new(SlateTable::new(db)),
            dictionaries: HashMap::new(),
        })
    }

    async fn dictionary_for(&mut self, namespace: &str) -> Result<Arc<Dictionary<Vec<u8>>>> {
        match self.dictionaries.entry(namespace.to_string()) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let dict = Arc::new(
                    Dictionary::with_table(self.table.clone(), namespace.to_string(), None).await?,
                );
                Ok(entry.insert(dict).clone())
            }
        }
    }

    /// Provisions a new [`ZSetStream`] in the provided namespace with the supplied retention policy.
    pub async fn new_stream(
        &mut self,
        namespace: impl Into<String>,
        retention: StreamRetention,
    ) -> Result<ZSetStream<Vec<u8>>> {
        let namespace = namespace.into();
        let dict = self.dictionary_for(&namespace).await?;
        ZSetStream::new(dict, self.table.clone(), namespace, retention).await
    }

    pub async fn new_view(&mut self, view_name: &str) -> Result<DbspView> {
        let namespace = namespaces::materialized_view(view_name)?;
        let zset = self
            .new_stream(
                namespace.clone(),
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await?;
        Ok(DbspView {
            name: view_name.to_string(),
            namespace,
            zset,
        })
    }

    pub fn table(&self) -> Arc<dyn KeyValueTable> {
        self.table.clone()
    }

    pub async fn handle_view_for(
        &mut self,
        namespace: &str,
        version: u64,
    ) -> Result<ZSetHandleView<Vec<u8>>> {
        let dict = self.dictionary_for(namespace).await?;
        Ok(ZSetHandleView::new(
            dict,
            self.table.clone(),
            namespace.to_string(),
            version,
        ))
    }

    pub async fn latest_view_handle(&mut self, namespace: &str) -> Result<ZSetHandle> {
        let dict = self.dictionary_for(namespace).await?;
        let mut stream = ZSetStream::new(
            dict,
            self.table.clone(),
            namespace.to_string(),
            StreamRetention::KeepLast { keep_last: 1 },
        )
        .await?;
        stream.latest_handle().await
    }
}

/// Mutable writer for a specific materialized view.
pub struct DbspView {
    name: String,
    namespace: String,
    zset: ZSetStream<Vec<u8>>,
}

impl DbspView {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn add_delta(&mut self, key: Vec<u8>, diff: i64) {
        self.zset.add_delta(key, diff);
    }

    pub fn add_deltas<I>(&mut self, deltas: I)
    where
        I: IntoIterator<Item = (Vec<u8>, i64)>,
    {
        self.zset.add_deltas(deltas);
    }

    pub async fn flush(&mut self) -> Result<ZSetHandle> {
        self.zset.flush().await
    }

    pub fn latest_handle_view(&self) -> ZSetHandleView<Vec<u8>> {
        self.zset.latest_view()
    }

    pub fn handle_stream(&self) -> Stream<ZSetHandle> {
        self.zset.handle_stream()
    }
}
