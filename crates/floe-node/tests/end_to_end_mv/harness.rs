use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Int64Builder, StringBuilder,
    TimestampMillisecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use dbsp::StreamRetention;
use floe_executor::encoding::{EncodedRowScalar, decode_all_encoded_row_scalars};
use floe_executor::{
    BuildInputs, DbspBridge, DbspGraphBuilder, FloeQueryContext, GraphTaskError, MaterializedView,
    MaterializedViewRegistry, OuterStreamRegistry, ValidatedPlan, load_or_register_mv,
    validate_dbsp_plan,
};
use floe_node::executor::{available_sources_from_registry, build_dataflows};
use floe_node::generator;
use floe_node::planner::plan_materialized_views;
use floe_node::source::SourceRegistry;
use floe_sql_parser::parse_materialized_view;
use floe_storage::SlateCatalog;
use slatedb::object_store::ObjectStore;
use slatedb::object_store::memory::InMemory;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

static NEXT_TEST_CATALOG_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) struct MvTestHarness {
    pub(crate) catalog: Arc<SlateCatalog>,
    pub(crate) db: Arc<slatedb::Db>,
    pub(crate) mv_registry: Arc<MaterializedViewRegistry>,
    pub(crate) outer: OuterStreamRegistry,
    pub(crate) ingestion_bridge: DbspBridge,
    pub(crate) view_name: String,
    cancel: CancellationToken,
}

impl MvTestHarness {
    pub(crate) async fn new(view_name: &str, view_sql: &str) -> Result<Self> {
        let catalog_id = NEXT_TEST_CATALOG_ID.fetch_add(1, Ordering::Relaxed);
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(
            SlateCatalog::with_object_store(format!("in-memory-test-{catalog_id}"), object_store)
                .await?,
        );
        Self::new_with_catalog(catalog, view_name, view_sql).await
    }

    pub(crate) async fn new_with_catalog(
        catalog: Arc<SlateCatalog>,
        view_name: &str,
        view_sql: &str,
    ) -> Result<Self> {
        let db = catalog.db();

        let mut registry = SourceRegistry::new();
        registry.extend(generator::definitions()?);
        let available_sources = available_sources_from_registry(&registry);

        let definition = parse_materialized_view(view_sql)?;
        let planned = plan_materialized_views(&registry, &[definition]).await?;
        assert_eq!(
            planned.len(),
            1,
            "expected a single planned materialized view"
        );
        let circuit_plans = build_dataflows(&planned, &available_sources, &registry)?;
        assert_eq!(
            circuit_plans.len(),
            1,
            "expected a single circuit plan for the view"
        );

        let ValidatedPlan {
            required_sources, ..
        } = validate_dbsp_plan(&circuit_plans[0], &available_sources, view_name)?;

        let mv_registry = Arc::new(MaterializedViewRegistry::new());
        let mut graph_builder = DbspGraphBuilder::new(Arc::clone(&db)).await?;
        let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let outer =
            OuterStreamRegistry::from_validated_sources(&required_sources, &mut ingestion_bridge)
                .await?;
        let source_refs: Vec<&str> = required_sources.iter().map(String::as_str).collect();
        let handle_streams = gather_handle_streams(&outer, &source_refs);
        let transient_streams = gather_transient_streams(&outer, &source_refs);
        let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
        let cancel = CancellationToken::new();
        graph_builder
            .build(BuildInputs {
                graph_id: view_name,
                view_name,
                plan: &circuit_plans[0],
                cancel: cancel.clone(),
                task_events: task_tx.clone(),
                mv_registry: Arc::clone(&mv_registry),
                outer_handle_streams: &handle_streams,
                outer_transient_streams: &transient_streams,
                enable_source_batch_journal: false,
                restore_transient_helper_state: false,
                mv_retention: StreamRetention::KeepLast { keep_last: 1 },
                watermark: Arc::new(AtomicI64::new(-1)),
            })
            .await?;

        Ok(Self {
            catalog,
            db,
            mv_registry,
            outer,
            ingestion_bridge,
            view_name: view_name.to_string(),
            cancel,
        })
    }

    pub(crate) async fn session_with_view(
        &self,
    ) -> Result<(datafusion::execution::context::SessionContext, DbspBridge)> {
        self.publish_arrow_snapshots_from_encoded_state().await?;
        let query = FloeQueryContext::new(Arc::clone(&self.catalog));
        let session = query.session();
        let mut bridge = DbspBridge::new(Arc::clone(&self.db)).await?;
        load_or_register_mv(
            &session,
            Arc::clone(&self.mv_registry),
            &mut bridge,
            &self.view_name,
        )
        .await?;
        Ok((session, bridge))
    }

    async fn publish_arrow_snapshots_from_encoded_state(&self) -> Result<()> {
        let handle = self
            .mv_registry
            .get(&self.view_name)
            .with_context(|| format!("materialized view handle for '{}'", self.view_name))?;
        let schema = self
            .mv_registry
            .schema(&self.view_name)
            .with_context(|| format!("schema for materialized view '{}'", self.view_name))?;

        let mut versions = Vec::new();
        let mut cursor = -1_i64;
        while let Some(version) = handle.next_version_after(cursor) {
            versions.push(version);
            cursor = version;
        }

        for version in versions {
            let state = if let Some((_base, _target, overlay)) = u64::try_from(version)
                .ok()
                .and_then(|as_of| handle.encoded_overlay_merged_delta(Some(as_of)))
            {
                overlay
            } else if version == 0 {
                std::collections::HashMap::new()
            } else {
                MaterializedView::handle_for(handle.as_ref(), version)?
                    .materialize()
                    .await?
            };
            let batches = encoded_state_to_arrow_batches(Arc::clone(&schema), state)?;
            handle.publish_arrow_version(version, batches, Vec::new());
        }
        Ok(())
    }
}

impl Drop for MvTestHarness {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

fn gather_handle_streams(
    outer: &OuterStreamRegistry,
    sources: &[&str],
) -> HashMap<String, dbsp::DeltaHandleStream> {
    let mut map = HashMap::new();
    for source in sources {
        if let Some(stream) = outer.delta_handle_stream(source) {
            map.insert((*source).to_string(), stream);
        }
    }
    map
}

fn gather_transient_streams(
    outer: &OuterStreamRegistry,
    sources: &[&str],
) -> HashMap<String, floe_executor::outer_stream::TransientSourceHandleStream> {
    let mut map = HashMap::new();
    for source in sources {
        if let Some(stream) = outer.transient_stream(source) {
            map.insert((*source).to_string(), stream);
        }
    }
    map
}

fn encoded_state_to_arrow_batches(
    schema: datafusion::arrow::datatypes::SchemaRef,
    state: std::collections::HashMap<Vec<u8>, i64>,
) -> Result<Vec<RecordBatch>> {
    let mut rows = Vec::new();
    for (encoded, diff) in state {
        if diff <= 0 {
            continue;
        }
        let decoded = decode_all_encoded_row_scalars(&encoded)?;
        for _ in 0..diff {
            rows.push(decoded.clone());
        }
    }
    let columns = (0..schema.fields().len())
        .map(|idx| build_arrow_column(schema.field(idx), &rows, idx))
        .collect::<Result<Vec<_>>>()?;
    Ok(vec![RecordBatch::try_new(schema, columns)?])
}

fn build_arrow_column(
    field: &datafusion::arrow::datatypes::Field,
    rows: &[Vec<Option<EncodedRowScalar>>],
    idx: usize,
) -> Result<ArrayRef> {
    match field.data_type() {
        DataType::Int64 => {
            let mut builder = Int64Builder::new();
            for row in rows {
                match row.get(idx).and_then(Option::as_ref) {
                    Some(EncodedRowScalar::Int64(value)) => builder.append_value(*value),
                    Some(other) => bail!(
                        "encoded MV column '{}' expected Int64, got {:?}",
                        field.name(),
                        other
                    ),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for row in rows {
                match row.get(idx).and_then(Option::as_ref) {
                    Some(EncodedRowScalar::Utf8(value)) => builder.append_value(value),
                    Some(other) => bail!(
                        "encoded MV column '{}' expected Utf8, got {:?}",
                        field.name(),
                        other
                    ),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for row in rows {
                match row.get(idx).and_then(Option::as_ref) {
                    Some(EncodedRowScalar::Bool(value)) => builder.append_value(*value),
                    Some(other) => bail!(
                        "encoded MV column '{}' expected Boolean, got {:?}",
                        field.name(),
                        other
                    ),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Date32 => {
            let mut builder = Date32Builder::new();
            for row in rows {
                match row.get(idx).and_then(Option::as_ref) {
                    Some(EncodedRowScalar::DateDays(value)) => builder.append_value(*value),
                    Some(other) => bail!(
                        "encoded MV column '{}' expected Date32, got {:?}",
                        field.name(),
                        other
                    ),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let mut builder = TimestampMillisecondBuilder::new();
            for row in rows {
                match row.get(idx).and_then(Option::as_ref) {
                    Some(EncodedRowScalar::TimestampMillis(value)) => builder.append_value(*value),
                    Some(other) => bail!(
                        "encoded MV column '{}' expected Timestamp(Millisecond), got {:?}",
                        field.name(),
                        other
                    ),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        other => bail!(
            "end-to-end MV harness cannot publish Arrow snapshots for column '{}' with type {:?}",
            field.name(),
            other
        ),
    }
}
