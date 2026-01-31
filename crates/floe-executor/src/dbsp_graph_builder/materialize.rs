use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, anyhow};
use dbsp::RowSchema;
use dbsp::handles::ZSetHandle;
use dbsp::storage::KeyValueTable;
use dbsp::storage::dictionary::Dictionary;
use dbsp::stream::util::materialize_zset_handle;
use dbsp::stream::{DeltaHandleStream, StreamCursor};
use dbsp::StreamRetention;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::dbsp_bridge::{DbspBridge, DbspView};
use crate::materialized_view::{DbspPersistedState, MaterializedViewRegistry};
use crate::task_events::{GraphTaskSender, report_graph_task_error};

use super::builder::DbspGraphBuilder;

static MV_UPDATE_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
const MV_UPDATE_LOG_SAMPLE_EVERY: u64 = 128;

impl DbspGraphBuilder {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn materialize_view(
        &mut self,
        view_name: &str,
        schema: Arc<RowSchema>,
        upstream: DeltaHandleStream,
        cancel: &CancellationToken,
        task_events: &GraphTaskSender,
        mv_registry: &Arc<MaterializedViewRegistry>,
        mv_latest: &mut HashMap<String, (i64, ZSetHandle)>,
        retention: StreamRetention,
    ) -> Result<DeltaHandleStream> {
        let handle_stream = upstream.clone();
        let registry_handle = mv_registry.register(view_name.to_string());
        let arrow_schema = schema.to_arrow_schema();
        mv_registry.set_schema(view_name.to_string(), Arc::clone(&arrow_schema));
        {
            let bridge = self.bridge.lock().await;
            bridge
                .save_mv_schema(view_name, Arc::clone(&arrow_schema))
                .await
                .with_context(|| format!("persist schema metadata for '{view_name}'"))?;
        }

        let mut view = {
            let mut bridge = self.bridge.lock().await;
            bridge
                .new_view(view_name, retention)
                .await
                .with_context(|| format!("provision materialized view '{view_name}'"))?
        };
        let mut view_handle_stream = view.handle_stream();
        let view_frontier = view_handle_stream.committed_frontier();
        if view_frontier >= 0 {
            let handle = view_handle_stream.get(view_frontier).await?;
            let state = self.state_from_handle(&handle).await?;
            registry_handle.set_dbsp_state(state);
            registry_handle.publish_version(view_frontier, handle.clone());
            mv_latest.insert(view_name.to_string(), (view_frontier, handle));
        }

        let registry_clone = registry_handle.clone();
        let table = {
            let bridge = self.bridge.lock().await;
            bridge.table()
        };
        let cursor = StreamCursor::new(upstream.stream());
        let upstream_frontier = cursor.observed();
        let mut upstream_stream = handle_stream.stream();
        let mut dict_cache: HashMap<String, Arc<Dictionary<Vec<u8>>>> = HashMap::new();
        if view_frontier < upstream_frontier {
            for ts in (view_frontier + 1)..=upstream_frontier {
                let delta_handle = upstream_stream
                    .get(ts)
                    .await
                    .with_context(|| format!("load delta handle for view '{view_name}' at {ts}"))?;
                let snapshot_handle = Self::apply_delta_handle_to_view(
                    &mut view,
                    table.clone(),
                    &mut dict_cache,
                    &delta_handle,
                )
                .await
                .with_context(|| format!("apply delta for view '{view_name}' at {ts}"))?;
                let state = self.state_from_handle(&snapshot_handle).await?;
                registry_handle.set_dbsp_state(state);
                registry_handle.publish_version(ts, snapshot_handle.clone());
                mv_latest.insert(view_name.to_string(), (ts, snapshot_handle));
            }
        }

        let bridge_clone = Arc::clone(&self.bridge);
        let view_label = view_name.to_string();
        let task_label = format!("materialize-view:{view_label}");
        let task_events = task_events.clone();
        let graph_id = self.graph_id().to_string();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let mut cursor = cursor;
            let mut view = view;
            let mut dict_cache = dict_cache;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    result = cursor.next() => {
                        match result {
                            Ok((ts, delta_handle)) => {
                                let snapshot_handle = match Self::apply_delta_handle_to_view(
                                    &mut view,
                                    table.clone(),
                                    &mut dict_cache,
                                    &delta_handle,
                                )
                                .await
                                {
                                    Ok(handle) => handle,
                                    Err(err) => {
                                        report_graph_task_error(
                                            &task_events,
                                            &graph_id,
                                            task_label.clone(),
                                            anyhow!(
                                                "failed to apply delta for materialized view '{view_label}' at {ts}: {err}"
                                            ),
                                        );
                                        break;
                                    }
                                };
                                match Self::state_from_handle_with_bridge(&bridge_clone, &snapshot_handle)
                                    .await
                                {
                                    Ok(state) => {
                                        registry_clone.set_dbsp_state(state);
                                        registry_clone.publish_version(ts, snapshot_handle);
                                        if MV_UPDATE_LOG_COUNTER
                                            .fetch_add(1, Ordering::Relaxed)
                                            .is_multiple_of(MV_UPDATE_LOG_SAMPLE_EVERY)
                                        {
                                            tracing::info!(
                                                view = %view_label,
                                                version = ts,
                                                "materialized view advanced"
                                            );
                                        }
                                    }
                                    Err(err) => {
                                        report_graph_task_error(
                                            &task_events,
                                            &graph_id,
                                            task_label.clone(),
                                            anyhow!(
                                                "failed to update materialized view '{view_label}': {err}"
                                            ),
                                        );
                                        break;
                                    }
                                }
                            }
                            Err(err) => {
                                report_graph_task_error(
                                    &task_events,
                                    &graph_id,
                                    task_label.clone(),
                                    anyhow!(
                                        "stream for materialized view '{view_label}' closed unexpectedly: {err}"
                                    ),
                                );
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(handle_stream)
    }

    async fn apply_delta_handle_to_view(
        view: &mut DbspView,
        table: Arc<dyn KeyValueTable>,
        dict_cache: &mut HashMap<String, Arc<Dictionary<Vec<u8>>>>,
        delta_handle: &ZSetHandle,
    ) -> Result<ZSetHandle> {
        let deltas = materialize_zset_handle::<Vec<u8>>(table, dict_cache, delta_handle)
            .await
            .context("materialize delta handle for materialized view")?;
        if !deltas.is_empty() {
            view.add_deltas(deltas);
        }
        view.flush()
            .await
            .context("flush materialized view updates")
    }

    async fn state_from_handle(&self, handle: &ZSetHandle) -> Result<DbspPersistedState> {
        Self::state_from_handle_with_bridge(&self.bridge, handle).await
    }

    async fn state_from_handle_with_bridge(
        bridge: &Arc<Mutex<DbspBridge>>,
        handle: &ZSetHandle,
    ) -> Result<DbspPersistedState> {
        let mut guard = bridge.lock().await;
        let handle_view = guard
            .handle_view_for(&handle.ns, handle.version)
            .await
            .context("open handle view for materialized view state")?;
        let (dict, table, namespace, version) = handle_view.into_parts();
        Ok(DbspPersistedState::new(dict, table, namespace, version))
    }
}
