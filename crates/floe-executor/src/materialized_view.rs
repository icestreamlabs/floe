use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::stream_types::{Diff, Row, Timestamp};

#[derive(Debug, Default)]
pub struct MaterializedViewRegistry {
    views: RwLock<HashMap<String, Arc<MaterializedViewHandle>>>,
}

impl MaterializedViewRegistry {
    pub fn new() -> Self {
        Self {
            views: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, name: impl Into<String>) -> Arc<MaterializedViewHandle> {
        let mut guard = self.views.write().expect("mutex poisoned");
        let name = name.into();
        guard
            .entry(name.clone())
            .or_insert_with(|| Arc::new(MaterializedViewHandle::new(name)))
            .clone()
    }

    pub fn get(&self, name: &str) -> Option<Arc<MaterializedViewHandle>> {
        self.views
            .read()
            .expect("mutex poisoned")
            .get(name)
            .cloned()
    }
}

#[derive(Debug)]
pub struct MaterializedViewHandle {
    name: String,
    state: RwLock<HashMap<Row, Diff>>,
    watermark: RwLock<Option<Timestamp>>,
}

impl MaterializedViewHandle {
    fn new(name: String) -> Self {
        Self {
            name,
            state: RwLock::new(HashMap::new()),
            watermark: RwLock::new(None),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn apply(&self, row: Row, diff: Diff) {
        if diff == 0 {
            return;
        }

        let mut guard = self.state.write().expect("mutex poisoned");
        let entry = guard.entry(row.clone()).or_insert(0);
        *entry += diff;
        if *entry == 0 {
            guard.remove(&row);
        }
    }

    pub fn update_watermark(&self, watermark: Timestamp) {
        *self.watermark.write().expect("mutex poisoned") = Some(watermark);
    }

    pub fn watermark(&self) -> Option<Timestamp> {
        *self.watermark.read().expect("mutex poisoned")
    }

    pub fn snapshot(&self) -> HashMap<Row, Diff> {
        self.state.read().expect("mutex poisoned").clone()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;

    use super::*;

    #[test]
    fn registers_and_updates_view_state() {
        let registry = MaterializedViewRegistry::new();
        let view = registry.register("mv_test");
        let row = vec![ScalarValue::Int64(Some(1))];
        view.apply(row.clone(), 1);
        assert_eq!(view.snapshot().get(&row), Some(&1));
        view.apply(row.clone(), -1);
        assert!(view.snapshot().is_empty());
        view.update_watermark(42);
        assert_eq!(view.watermark(), Some(42));
    }

    #[test]
    fn registry_returns_same_handle() {
        let registry = MaterializedViewRegistry::new();
        let view_a = registry.register("mv");
        let view_b = registry.get("mv").expect("view registered");
        assert!(Arc::ptr_eq(&view_a, &view_b));
    }
}
