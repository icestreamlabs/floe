use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::scalar::ScalarValue;
use dbsp::RowSchema;
use dbsp::StreamRetention;
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::handles::ZSetHandle;
use dbsp::storage::KeyValueTable;
use dbsp::storage::dictionary::Dictionary;
use dbsp::stream::util::materialize_zset_handle;
use dbsp::stream::{DeltaHandleStream, StreamCursor};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::dbsp_bridge::{DbspBridge, DbspView};
use crate::delta_batch::{DeltaBatchBuffer, DeltaBatchConfig};
use crate::delta_consolidation::{ConsolidationMode, DeltaConsolidator};
use crate::encoding::{decode_projected_row_key, encode_projected_row_key};
use crate::materialized_view::{DbspPersistedState, MaterializedViewRegistry};
use crate::metrics;
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
        consolidation_mode: ConsolidationMode,
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
        let graph_id = self.graph_id().to_string();
        let view_namespace = crate::namespaces::materialized_view(view_name)
            .unwrap_or_else(|_| format!("materialized_view/{view_name}"));
        if view_frontier < upstream_frontier {
            for ts in (view_frontier + 1)..=upstream_frontier {
                let update_start = Instant::now();
                let update_span = tracing::info_span!(
                    "dbsp_write",
                    graph_id = %graph_id,
                    view = %view_name,
                    namespace = %view_namespace,
                    version = ts,
                );
                let _enter = update_span.enter();
                let delta_handle = upstream_stream
                    .get(ts)
                    .await
                    .with_context(|| format!("load delta handle for view '{view_name}' at {ts}"))?;
                let snapshot_handle = Self::apply_delta_handle_to_view(
                    &mut view,
                    table.clone(),
                    &mut dict_cache,
                    Arc::clone(&arrow_schema),
                    consolidation_mode,
                    &delta_handle,
                )
                .await
                .with_context(|| format!("apply delta for view '{view_name}' at {ts}"))?;
                let state = self.state_from_handle(&snapshot_handle).await?;
                registry_handle.set_dbsp_state(state);
                registry_handle.publish_version(ts, snapshot_handle.clone());
                mv_latest.insert(view_name.to_string(), (ts, snapshot_handle));
                let latency_ms = update_start.elapsed().as_millis() as u64;
                metrics::observe_mv_update_latency_ms(latency_ms);
                metrics::inc_mv_updates();
                tracing::debug!(latency_ms, "materialized view update applied");
            }
        }

        let bridge_clone = Arc::clone(&self.bridge);
        let view_label = view_name.to_string();
        let view_namespace_label = view_namespace.clone();
        let task_label = format!("materialize-view:{view_label}");
        let task_events = task_events.clone();
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
                                let update_start = Instant::now();
                                let update_span = tracing::info_span!(
                                    "dbsp_write",
                                    graph_id = %graph_id,
                                    view = %view_label,
                                    namespace = %view_namespace_label,
                                    version = ts,
                                );
                                let _enter = update_span.enter();
                                let snapshot_handle = match Self::apply_delta_handle_to_view(
                                    &mut view,
                                    table.clone(),
                                    &mut dict_cache,
                                    Arc::clone(&arrow_schema),
                                    consolidation_mode,
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
                                        let latency_ms = update_start.elapsed().as_millis() as u64;
                                        metrics::observe_mv_update_latency_ms(latency_ms);
                                        metrics::inc_mv_updates();
                                        tracing::debug!(
                                            latency_ms,
                                            "materialized view update applied"
                                        );
                                        if MV_UPDATE_LOG_COUNTER
                                            .fetch_add(1, Ordering::Relaxed)
                                            .is_multiple_of(MV_UPDATE_LOG_SAMPLE_EVERY)
                                        {
                                            tracing::info!(
                                                view = %view_label,
                                                namespace = %view_namespace_label,
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
        row_schema: SchemaRef,
        consolidation_mode: ConsolidationMode,
        delta_handle: &ZSetHandle,
    ) -> Result<ZSetHandle> {
        let deltas = materialize_zset_handle::<Vec<u8>>(table, dict_cache, delta_handle)
            .await
            .context("materialize delta handle for materialized view")?;
        let include_key = consolidation_mode == ConsolidationMode::ByKey;
        let mut delta_batches = deltas_to_batches(&deltas, Arc::clone(&row_schema), include_key)
            .context("build delta record batches for consolidation")?;
        if !delta_batches.is_empty() {
            let consolidator =
                DeltaConsolidator::with_mode(delta_batches[0].schema(), consolidation_mode)
                    .context("build consolidator")?;
            let consolidation_start = Instant::now();
            let consolidated = consolidator
                .consolidate_with_stats(delta_batches)
                .await
                .context("consolidate output deltas before state write")?;
            metrics::observe_delta_consolidation(
                consolidated.stats,
                consolidation_start.elapsed().as_millis() as u64,
            );
            delta_batches = consolidated.batches;
            let consolidated =
                batches_to_deltas(&delta_batches, Arc::clone(&row_schema), include_key)
                    .with_context(|| {
                        format!("decode consolidated delta batches for '{}'", view.name())
                    })?;
            if !consolidated.is_empty() {
                view.add_deltas(consolidated);
            }
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

fn deltas_to_batches(
    deltas: &HashMap<Vec<u8>, i64>,
    row_schema: SchemaRef,
    include_key: bool,
) -> Result<Vec<RecordBatch>> {
    if deltas.is_empty() {
        return Ok(Vec::new());
    }

    let config = DeltaBatchConfig {
        max_rows: usize::MAX,
        max_bytes: usize::MAX,
    };
    let mut buffer = DeltaBatchBuffer::new(row_schema, include_key, config)
        .context("initialize delta batch buffer")?;
    for (key, diff) in deltas {
        let row = decode_projected_row_key(key).context("decode output row for consolidation")?;
        let key_bytes = include_key.then(|| key.clone());
        let _ = buffer
            .push(row, *diff, key_bytes)
            .context("append output row to consolidation batch")?;
    }

    let batch = buffer
        .flush_manual()
        .context("flush consolidation batch buffer")?;
    Ok(batch.into_iter().collect())
}

fn batches_to_deltas(
    batches: &[RecordBatch],
    row_schema: SchemaRef,
    include_key: bool,
) -> Result<HashMap<Vec<u8>, i64>> {
    let mut deltas = HashMap::new();
    let weight_idx = if include_key {
        row_schema.fields().len() + 1
    } else {
        row_schema.fields().len()
    };
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(row_schema.fields().len());
            for col_idx in 0..row_schema.fields().len() {
                row.push(
                    ScalarValue::try_from_array(batch.column(col_idx), row_idx)
                        .context("read consolidated row value")?,
                );
            }
            let weight = ScalarValue::try_from_array(batch.column(weight_idx), row_idx)
                .context("read consolidated weight value")?;
            let ScalarValue::Int64(Some(weight)) = weight else {
                return Err(anyhow!(
                    "{} column must contain non-null Int64 values",
                    WEIGHT_COLUMN_NAME
                ));
            };
            if weight == 0 {
                continue;
            }
            let encoded =
                encode_projected_row_key(&row).context("encode consolidated output row")?;
            let entry = deltas.entry(encoded.clone()).or_insert(0);
            *entry += weight;
            if *entry == 0 {
                deltas.remove(&encoded);
            }
        }
    }
    Ok(deltas)
}
