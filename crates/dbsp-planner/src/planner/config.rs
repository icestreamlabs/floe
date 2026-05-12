use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::TableReference;

use dbsp_circuit::circuit::tables::TableDescriptor;

#[derive(Debug, Clone)]
pub struct PlannerConfig {
    tables: HashMap<String, Arc<TableDescriptor>>,
}

impl PlannerConfig {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn with_table(mut self, table: &'static TableDescriptor) -> Self {
        self.register_table(table);
        self
    }

    pub fn register_table(&mut self, table: &'static TableDescriptor) {
        self.register_owned_table(table.clone());
    }

    pub fn register_alias(&mut self, alias: &str, table: &'static TableDescriptor) {
        self.tables
            .insert(alias.to_string(), Arc::new(table.clone()));
    }

    pub fn register_owned_table(&mut self, table: TableDescriptor) {
        self.tables.insert(table.name.to_string(), Arc::new(table));
    }

    pub(super) fn table(&self, name: &TableReference) -> Option<Arc<TableDescriptor>> {
        self.tables
            .get(name.table())
            .cloned()
            .or_else(|| self.tables.get(&name.to_string()).cloned())
    }
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self::new()
    }
}
