use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::TableReference;

use dbsp_circuit::circuit::tables::TableDescriptor;

const DEFAULT_OPTIMIZER_MAX_DUPLICATED_INPUTS: usize = 8;
const DEFAULT_OPTIMIZER_MAX_DUPLICATED_EXPR_NODES: usize = 128;

#[derive(Debug, Clone)]
pub struct PlannerConfig {
    tables: HashMap<String, Arc<TableDescriptor>>,
    disabled_optimizer_rules: HashSet<String>,
    optimizer_diagnostics: bool,
    optimizer_max_duplicated_inputs: usize,
    optimizer_max_duplicated_expr_nodes: usize,
}

impl PlannerConfig {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            disabled_optimizer_rules: HashSet::new(),
            optimizer_diagnostics: false,
            optimizer_max_duplicated_inputs: DEFAULT_OPTIMIZER_MAX_DUPLICATED_INPUTS,
            optimizer_max_duplicated_expr_nodes: DEFAULT_OPTIMIZER_MAX_DUPLICATED_EXPR_NODES,
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
        self.tables
            .insert(table.name().to_string(), Arc::new(table));
    }

    pub fn with_disabled_optimizer_rule(mut self, rule_name: impl Into<String>) -> Self {
        self.disable_optimizer_rule(rule_name);
        self
    }

    pub fn disable_optimizer_rule(&mut self, rule_name: impl Into<String>) {
        self.disabled_optimizer_rules.insert(rule_name.into());
    }

    pub fn disabled_optimizer_rules(&self) -> impl Iterator<Item = &str> {
        self.disabled_optimizer_rules.iter().map(String::as_str)
    }

    pub fn with_optimizer_diagnostics(mut self, enabled: bool) -> Self {
        self.set_optimizer_diagnostics(enabled);
        self
    }

    pub fn set_optimizer_diagnostics(&mut self, enabled: bool) {
        self.optimizer_diagnostics = enabled;
    }

    pub fn with_optimizer_max_duplicated_inputs(mut self, max_inputs: usize) -> Self {
        self.set_optimizer_max_duplicated_inputs(max_inputs);
        self
    }

    pub fn set_optimizer_max_duplicated_inputs(&mut self, max_inputs: usize) {
        self.optimizer_max_duplicated_inputs = max_inputs;
    }

    pub fn with_optimizer_max_duplicated_expr_nodes(mut self, max_nodes: usize) -> Self {
        self.set_optimizer_max_duplicated_expr_nodes(max_nodes);
        self
    }

    pub fn set_optimizer_max_duplicated_expr_nodes(&mut self, max_nodes: usize) {
        self.optimizer_max_duplicated_expr_nodes = max_nodes;
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

    pub(super) fn optimizer_max_duplicated_inputs(&self) -> usize {
        self.optimizer_max_duplicated_inputs
    }

    pub(super) fn optimizer_max_duplicated_expr_nodes(&self) -> usize {
        self.optimizer_max_duplicated_expr_nodes
    }
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self::new()
    }
}
