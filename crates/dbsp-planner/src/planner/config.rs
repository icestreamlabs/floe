use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::TableReference;

use dbsp_circuit::circuit::tables::TableDescriptor;

#[derive(Debug, Clone)]
pub struct PlannerConfig {
    tables: HashMap<String, Arc<TableDescriptor>>,
    disabled_optimizer_rules: HashSet<String>,
    optimizer_diagnostics: bool,
}

impl PlannerConfig {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            disabled_optimizer_rules: HashSet::new(),
            optimizer_diagnostics: false,
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

    pub fn with_disabled_optimizer_rule(mut self, rule_name: impl Into<String>) -> Self {
        self.disable_optimizer_rule(rule_name);
        self
    }

    pub fn disable_optimizer_rule(&mut self, rule_name: impl Into<String>) {
        self.disabled_optimizer_rules.insert(rule_name.into());
    }

    pub fn with_optimizer_diagnostics(mut self, enabled: bool) -> Self {
        self.set_optimizer_diagnostics(enabled);
        self
    }

    pub fn set_optimizer_diagnostics(&mut self, enabled: bool) {
        self.optimizer_diagnostics = enabled;
    }

    pub(super) fn table(&self, name: &TableReference) -> Option<Arc<TableDescriptor>> {
        self.tables
            .get(name.table())
            .cloned()
            .or_else(|| self.tables.get(&name.to_string()).cloned())
    }

    pub(super) fn optimizer_rule_enabled(&self, rule_name: &str) -> bool {
        !self.disabled_optimizer_rules.contains(rule_name)
    }

    pub(super) fn optimizer_diagnostics_enabled(&self) -> bool {
        self.optimizer_diagnostics
    }
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self::new()
    }
}
