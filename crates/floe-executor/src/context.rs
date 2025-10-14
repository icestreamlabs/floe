use std::sync::Arc;

use anyhow::{Result, anyhow};
use datafusion::execution::context::SessionContext;
use floe_core::catalog::TableDefinition;
use floe_storage::SlateCatalog;

use crate::table_provider::SlateTableProvider;

#[derive(Clone)]
pub struct FloeQueryContext {
    storage: Arc<SlateCatalog>,
    session: SessionContext,
}

impl FloeQueryContext {
    pub fn new(storage: Arc<SlateCatalog>) -> Self {
        Self {
            storage,
            session: SessionContext::new(),
        }
    }

    pub fn session(&self) -> SessionContext {
        self.session.clone()
    }

    pub async fn register_table(&self, table: TableDefinition) -> Result<()> {
        self.storage.upsert_table(table.clone()).await?;
        let provider = SlateTableProvider::new(self.storage.clone(), table.clone());
        self.session
            .register_table(table.name(), Arc::new(provider))
            .map_err(|err| anyhow!(err.to_string()))?;
        Ok(())
    }

    pub async fn preload_tables(&self) -> Result<()> {
        let tables = self.storage.tables().await?;
        for table in tables {
            let provider = SlateTableProvider::new(self.storage.clone(), table.clone());
            self.session
                .register_table(table.name(), Arc::new(provider))
                .map_err(|err| anyhow!(err.to_string()))?;
        }
        Ok(())
    }

    pub fn storage(&self) -> Arc<SlateCatalog> {
        self.storage.clone()
    }
}
