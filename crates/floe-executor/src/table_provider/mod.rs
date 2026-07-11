mod dynamic_state;
mod filters;
mod helpers;
mod materialized_view;
mod scan_exec;
mod slate;

#[cfg(test)]
mod tests;

const MV_VERSION_COLUMN: &str = "__mv_version";

pub(crate) use dynamic_state::{
    DynamicStateKey, DynamicStateTableProvider, encode_dynamic_state_keys,
};
pub use materialized_view::MaterializedViewTableProvider;
pub(crate) use scan_exec::SnapshotScanExec;
pub use slate::SlateTableProvider;
