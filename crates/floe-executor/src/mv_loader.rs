use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use datafusion::execution::context::SessionContext;

use crate::dbsp_bridge::DbspBridge;
use crate::materialized_view::{DbspPersistedState, MaterializedViewRegistry};
use crate::namespaces;
use crate::table_provider::MaterializedViewTableProvider;

pub async fn load_or_register_mv(
    session: &SessionContext,
    registry: Arc<MaterializedViewRegistry>,
    bridge: &mut DbspBridge,
    view_name: &str,
) -> Result<()> {
    if session.table(view_name).await.is_ok() {
        return Ok(());
    }

    let schema = registry.schema(view_name).ok_or_else(|| {
        anyhow!(
            "materialized view '{view}' is missing schema; ensure it was planned before loading",
            view = view_name
        )
    })?;

    let handle = registry.register(view_name.to_string());
    if handle.dbsp_state().is_none() {
        let namespace = namespaces::materialized_view(view_name)
            .context("derive namespace for materialized view")?;
        let latest_handle = bridge
            .latest_view_handle(&namespace)
            .await
            .with_context(|| format!("load latest handle for materialized view '{view_name}'"))?;
        let handle_view = bridge
            .handle_view_for(&latest_handle.ns, latest_handle.version)
            .await
            .with_context(|| {
                format!(
                    "open handle view for materialized view '{view}' (version {})",
                    latest_handle.version,
                    view = view_name
                )
            })?;
        let (dict, table, ns, version) = handle_view.into_parts();
        let state = DbspPersistedState::new(dict, table, ns, version);
        handle.set_dbsp_state(state);
    }

    let provider = MaterializedViewTableProvider::new(
        Arc::clone(&registry),
        view_name.to_string(),
        schema.clone(),
    );
    session
        .register_table(view_name, Arc::new(provider))
        .with_context(|| format!("register materialized view '{view_name}'"))?;
    Ok(())
}
