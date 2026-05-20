use std::collections::HashMap;

use floe_cdc_core::{CdcTableId, CdcTableSchema, UpstreamTableRef};

#[derive(Debug, Clone, Default)]
pub struct PostgresTableRouter {
    by_upstream_table: HashMap<UpstreamTableRef, CdcTableId>,
}

impl PostgresTableRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, upstream_table: UpstreamTableRef, table_id: CdcTableId) {
        self.by_upstream_table.insert(upstream_table, table_id);
    }

    pub fn get(&self, upstream_table: &UpstreamTableRef) -> Option<&CdcTableId> {
        self.by_upstream_table.get(upstream_table)
    }

    pub fn from_schemas<'a>(schemas: impl IntoIterator<Item = &'a CdcTableSchema>) -> Self {
        let mut router = Self::new();
        for schema in schemas {
            router.insert(schema.upstream_table().clone(), schema.table_id().clone());
        }
        router
    }
}
