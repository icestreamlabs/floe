use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use crate::metrics;

pub(crate) fn record_runtime_failure(
    component: &'static str,
    state: &Arc<StdMutex<Option<String>>>,
    message: String,
) {
    metrics::inc_runtime_error(component);
    let mut guard = match state.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!(
                component,
                "runtime failure state lock was poisoned; preserving first failure"
            );
            poisoned.into_inner()
        }
    };
    if guard.is_none() {
        *guard = Some(message);
    }
}
