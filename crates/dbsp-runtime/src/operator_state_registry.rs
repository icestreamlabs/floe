use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::handles::ZSetHandle;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OperatorStateHandle {
    pub name: String,
    pub namespace: String,
    pub version: u64,
}

impl OperatorStateHandle {
    pub fn new(name: impl Into<String>, namespace: impl Into<String>, version: u64) -> Self {
        Self {
            name: name.into(),
            namespace: namespace.into(),
            version,
        }
    }

    pub fn zset_handle(&self) -> ZSetHandle {
        ZSetHandle {
            ns: self.namespace.clone(),
            version: self.version,
        }
    }
}

#[derive(Default)]
struct OperatorStateRegistry {
    live: HashMap<String, OperatorStateHandle>,
    restore: HashMap<String, OperatorStateHandle>,
}

static REGISTRY: OnceLock<Mutex<OperatorStateRegistry>> = OnceLock::new();

fn registry() -> &'static Mutex<OperatorStateRegistry> {
    REGISTRY.get_or_init(|| Mutex::new(OperatorStateRegistry::default()))
}

pub fn record_operator_state(name: impl Into<String>, handle: ZSetHandle) {
    let handle = OperatorStateHandle::new(name, handle.ns.clone(), handle.version);
    let mut guard = registry()
        .lock()
        .expect("operator state registry lock poisoned");
    guard.live.insert(handle.namespace.clone(), handle);
}

pub fn snapshot_operator_states() -> Vec<OperatorStateHandle> {
    let guard = registry()
        .lock()
        .expect("operator state registry lock poisoned");
    let mut handles = guard.live.values().cloned().collect::<Vec<_>>();
    handles.sort_by(|left, right| {
        left.namespace
            .cmp(&right.namespace)
            .then(left.name.cmp(&right.name))
    });
    handles
}

pub fn install_operator_state_restore(handles: Vec<OperatorStateHandle>) {
    let mut guard = registry()
        .lock()
        .expect("operator state registry lock poisoned");
    guard.restore = handles
        .into_iter()
        .map(|handle| (handle.namespace.clone(), handle))
        .collect();
    guard.live.clear();
}

pub fn restored_operator_state(namespace: &str) -> Option<OperatorStateHandle> {
    let guard = registry()
        .lock()
        .expect("operator state registry lock poisoned");
    guard.restore.get(namespace).cloned()
}

#[cfg(test)]
pub fn clear_operator_state_registry() {
    let mut guard = registry()
        .lock()
        .expect("operator state registry lock poisoned");
    guard.live.clear();
    guard.restore.clear();
}
