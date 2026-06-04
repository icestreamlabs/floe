use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, OnceLock};

use crate::handles::ZSetHandle;

const UNCHECKPOINTED_OPERATOR_STATE_PREFIX: &str = "__uncheckpointed_operator_state/";

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

pub fn uncheckpointed_operator_state_namespace(namespace: impl AsRef<str>) -> String {
    format!(
        "{UNCHECKPOINTED_OPERATOR_STATE_PREFIX}{}",
        namespace.as_ref()
    )
}

pub fn is_checkpointed_operator_state_namespace(namespace: &str) -> bool {
    !namespace.starts_with(UNCHECKPOINTED_OPERATOR_STATE_PREFIX)
}

#[derive(Default)]
struct OperatorStateRegistry {
    live_by_graph: HashMap<String, HashMap<String, OperatorStateHandle>>,
    restore_by_graph: HashMap<String, HashMap<String, OperatorStateHandle>>,
}

static REGISTRY: OnceLock<Mutex<OperatorStateRegistry>> = OnceLock::new();

fn registry() -> &'static Mutex<OperatorStateRegistry> {
    REGISTRY.get_or_init(|| Mutex::new(OperatorStateRegistry::default()))
}

fn registry_guard() -> MutexGuard<'static, OperatorStateRegistry> {
    match registry().lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!("operator state registry lock was poisoned; recovering inner state");
            poisoned.into_inner()
        }
    }
}

fn graph_id_for_namespace(namespace: &str) -> Option<&str> {
    let mut parts = namespace.strip_prefix("op/")?.split('/');
    let graph_id = parts.next().filter(|graph_id| !graph_id.is_empty())?;
    let operator = parts.next().filter(|operator| !operator.is_empty())?;
    let side = parts.next().filter(|side| !side.is_empty())?;
    if parts.next().is_some() || operator.is_empty() || side.is_empty() {
        return None;
    }
    Some(graph_id)
}

pub fn record_operator_state(name: impl Into<String>, handle: ZSetHandle) {
    if !is_checkpointed_operator_state_namespace(&handle.ns) {
        return;
    }
    let handle = OperatorStateHandle::new(name, handle.ns.clone(), handle.version);
    let Some(graph_id) = graph_id_for_namespace(&handle.namespace) else {
        tracing::warn!(
            namespace = %handle.namespace,
            "ignoring operator state handle with invalid checkpoint namespace"
        );
        return;
    };
    let mut guard = registry_guard();
    guard
        .live_by_graph
        .entry(graph_id.to_string())
        .or_default()
        .insert(handle.namespace.clone(), handle);
}

pub fn snapshot_operator_states() -> Vec<OperatorStateHandle> {
    let guard = registry_guard();
    let mut handles = guard
        .live_by_graph
        .values()
        .flat_map(|handles| handles.values().cloned())
        .collect::<Vec<_>>();
    sort_operator_state_handles(&mut handles);
    handles
}

pub fn snapshot_operator_states_for_graph(graph_id: &str) -> Vec<OperatorStateHandle> {
    let guard = registry_guard();
    let mut handles = guard
        .live_by_graph
        .get(graph_id)
        .into_iter()
        .flat_map(|handles| handles.values().cloned())
        .collect::<Vec<_>>();
    sort_operator_state_handles(&mut handles);
    handles
}

fn sort_operator_state_handles(handles: &mut [OperatorStateHandle]) {
    handles.sort_by(|left, right| {
        left.namespace
            .cmp(&right.namespace)
            .then(left.name.cmp(&right.name))
    });
}

pub fn install_operator_state_restore(handles: Vec<OperatorStateHandle>) {
    let mut restore_by_graph: HashMap<String, HashMap<String, OperatorStateHandle>> =
        HashMap::new();
    for handle in handles {
        if !is_checkpointed_operator_state_namespace(&handle.namespace) {
            continue;
        }
        let Some(graph_id) = graph_id_for_namespace(&handle.namespace) else {
            tracing::warn!(
                namespace = %handle.namespace,
                "ignoring restored operator state handle with invalid checkpoint namespace"
            );
            continue;
        };
        restore_by_graph
            .entry(graph_id.to_string())
            .or_default()
            .insert(handle.namespace.clone(), handle);
    }
    let mut guard = registry_guard();
    guard.restore_by_graph = restore_by_graph;
    guard.live_by_graph.clear();
}

pub fn install_operator_state_restore_for_graph(graph_id: &str, handles: Vec<OperatorStateHandle>) {
    let restore = handles
        .into_iter()
        .filter(|handle| is_checkpointed_operator_state_namespace(&handle.namespace))
        .filter(|handle| graph_id_for_namespace(&handle.namespace) == Some(graph_id))
        .map(|handle| (handle.namespace.clone(), handle))
        .collect();
    let mut guard = registry_guard();
    guard.restore_by_graph.insert(graph_id.to_string(), restore);
    guard.live_by_graph.remove(graph_id);
}

pub fn restored_operator_state(namespace: &str) -> Option<OperatorStateHandle> {
    if !is_checkpointed_operator_state_namespace(namespace) {
        return None;
    }
    let graph_id = graph_id_for_namespace(namespace)?;
    let guard = registry_guard();
    guard
        .restore_by_graph
        .get(graph_id)
        .and_then(|handles| handles.get(namespace))
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uncheckpointed_operator_state_is_excluded_from_snapshots_and_restore() {
        let scratch_namespace = uncheckpointed_operator_state_namespace("scratch");
        record_operator_state(
            "scratch",
            ZSetHandle {
                ns: scratch_namespace.clone(),
                version: 7,
            },
        );
        assert!(
            !snapshot_operator_states()
                .iter()
                .any(|handle| handle.namespace == scratch_namespace)
        );

        let checkpointed =
            OperatorStateHandle::new("stable", "op/graph_uncheckpointed_restore/0/stable", 11);
        install_operator_state_restore_for_graph(
            "graph_uncheckpointed_restore",
            vec![
                OperatorStateHandle::new("scratch", scratch_namespace.clone(), 7),
                checkpointed.clone(),
            ],
        );

        assert!(restored_operator_state(&scratch_namespace).is_none());
        assert_eq!(
            restored_operator_state("op/graph_uncheckpointed_restore/0/stable"),
            Some(checkpointed)
        );
    }

    #[test]
    fn invalid_checkpoint_namespace_is_ignored() {
        record_operator_state(
            "invalid",
            ZSetHandle {
                ns: "stable_namespace".to_string(),
                version: 11,
            },
        );
        assert!(
            !snapshot_operator_states()
                .iter()
                .any(|handle| handle.namespace == "stable_namespace")
        );

        install_operator_state_restore_for_graph(
            "invalid_checkpoint_namespace",
            vec![OperatorStateHandle::new("invalid", "stable_namespace", 11)],
        );
        assert!(restored_operator_state("stable_namespace").is_none());
    }

    #[test]
    fn graph_scoped_snapshot_and_restore_do_not_bleed_between_graphs() {
        record_operator_state(
            "left",
            ZSetHandle {
                ns: "op/graph_a/1/left".to_string(),
                version: 2,
            },
        );
        record_operator_state(
            "left",
            ZSetHandle {
                ns: "op/graph_b/1/left".to_string(),
                version: 5,
            },
        );

        assert_eq!(snapshot_operator_states_for_graph("graph_a").len(), 1);
        assert_eq!(snapshot_operator_states_for_graph("graph_b").len(), 1);

        install_operator_state_restore_for_graph(
            "graph_a",
            vec![OperatorStateHandle::new("left", "op/graph_a/1/left", 2)],
        );

        assert_eq!(
            restored_operator_state("op/graph_a/1/left"),
            Some(OperatorStateHandle::new("left", "op/graph_a/1/left", 2))
        );
        assert!(restored_operator_state("op/graph_b/1/left").is_none());
    }
}
