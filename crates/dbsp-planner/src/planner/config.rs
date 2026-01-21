use std::collections::HashMap;

use datafusion::common::TableReference;

use dbsp_circuit::circuit::tables::TableDescriptor;

#[derive(Debug, Clone)]
pub struct PlannerConfig {
    tables: HashMap<String, &'static TableDescriptor>,
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
        self.tables.insert(table.name.to_string(), table);
    }

    pub fn register_alias(&mut self, alias: &str, table: &'static TableDescriptor) {
        self.tables.insert(alias.to_string(), table);
    }

    pub(super) fn table(&self, name: &TableReference) -> Option<&'static TableDescriptor> {
        self.tables
            .get(name.table())
            .copied()
            .or_else(|| self.tables.get(&name.to_string()).copied())
    }
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self::new()
    }
}
