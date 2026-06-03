use std::sync::Arc;

use dbsp::storage::KeyValueTable;
use dbsp::storage::dictionary::Dictionary;

#[derive(Clone)]
pub struct DbspPersistedState {
    dictionary: Arc<Dictionary<Vec<u8>>>,
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    version: u64,
    logical_version: u64,
}

impl std::fmt::Debug for DbspPersistedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbspPersistedState")
            .field("namespace", &self.namespace)
            .field("version", &self.version)
            .field("logical_version", &self.logical_version)
            .finish()
    }
}

impl DbspPersistedState {
    pub fn new(
        dictionary: Arc<Dictionary<Vec<u8>>>,
        table: Arc<dyn KeyValueTable>,
        namespace: String,
        version: u64,
    ) -> Self {
        Self {
            dictionary,
            table,
            namespace,
            version,
            logical_version: version,
        }
    }

    pub fn dictionary(&self) -> Arc<Dictionary<Vec<u8>>> {
        Arc::clone(&self.dictionary)
    }

    pub fn table(&self) -> Arc<dyn KeyValueTable> {
        self.table.clone()
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn with_logical_version(mut self, logical_version: u64) -> Self {
        self.logical_version = logical_version;
        self
    }

    pub fn logical_version(&self) -> u64 {
        self.logical_version
    }
}
