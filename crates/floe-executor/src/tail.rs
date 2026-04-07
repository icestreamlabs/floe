use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context as TaskContext, Poll};
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail, ensure};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use futures::Stream;
#[cfg(test)]
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::encoding::decode_all_encoded_row_scalars;
use crate::materialized_view::{MaterializedViewHandle, MaterializedViewRegistry};
use crate::metrics;
use crate::mv::runtime::MaterializedView;
use crate::scalar_array_builder::ScalarColumnBuilder;
use floe_sql_parser::{FloeStatement, parse_floe_statement};

/// Alias for the DataFusion session context.
pub type SessionCtx = SessionContext;

pub type PgResult<T> = Result<T>;
const TAIL_STREAM_CHANNEL_CAPACITY_DEFAULT: usize = 256;
const TAIL_MAX_CATCHUP_VERSIONS_DEFAULT: i64 = 32;
const TAIL_DELTA_LOG_SAMPLE_EVERY: usize = 128;
static TAIL_DELTA_LOG_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
pub struct TailBatch {
    pub version: i64,
    pub batch: RecordBatch,
    pub ops: Vec<i16>,
    pub times: Vec<Option<i64>>,
}

#[derive(Debug)]
struct TailCanceledError;

impl fmt::Display for TailCanceledError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "query canceled")
    }
}

impl std::error::Error for TailCanceledError {}

fn query_canceled_error() -> anyhow::Error {
    anyhow::Error::new(TailCanceledError)
}

pub fn is_tail_canceled_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<TailCanceledError>().is_some()
}

fn build_tail_schema(user_schema: &SchemaRef) -> SchemaRef {
    let mut fields = Vec::with_capacity(user_schema.fields().len() + 3);
    fields.push(Field::new("__mv_version", DataType::Int64, false));
    fields.push(Field::new("__op", DataType::Int16, false));
    fields.push(Field::new(
        "__time",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        true,
    ));
    fields.extend(user_schema.fields().iter().map(|field| (**field).clone()));
    Arc::new(Schema::new(fields))
}

#[derive(Debug, Clone)]
pub struct TailParams {
    pub mv_name: String,
    pub with_snapshot: bool,
    pub as_of: Option<i64>,
}

#[derive(Debug)]
pub struct TailStream {
    schema: SchemaRef,
    receiver: mpsc::Receiver<PgResult<TailBatch>>,
}

impl TailStream {
    fn new(schema: SchemaRef, receiver: mpsc::Receiver<PgResult<TailBatch>>) -> Self {
        Self { schema, receiver }
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for TailStream {
    type Item = PgResult<TailBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_recv(cx)
    }
}

pub trait MvCatalog: Send + Sync {
    type View: MaterializedView + Send + Sync + 'static;

    fn materialized_view(&self, name: &str) -> Option<Arc<Self::View>>;
    fn schema(&self, name: &str) -> Option<SchemaRef>;
}

impl MvCatalog for MaterializedViewRegistry {
    type View = MaterializedViewHandle;

    fn materialized_view(&self, name: &str) -> Option<Arc<Self::View>> {
        self.get(name)
    }

    fn schema(&self, name: &str) -> Option<SchemaRef> {
        self.schema(name)
    }
}

pub async fn execute_tail<C>(
    ctx: &SessionCtx,
    catalog: &C,
    params: TailParams,
    cancel: CancellationToken,
) -> PgResult<TailStream>
where
    C: MvCatalog + ?Sized,
{
    crate::metrics::init();
    let _ = ctx;
    let mv = catalog
        .materialized_view(&params.mv_name)
        .ok_or_else(|| anyhow!("materialized view '{}' not found", params.mv_name))?;
    let output_schema = tail_output_schema(catalog, &params.mv_name)?;
    let base_schema = catalog.schema(&params.mv_name).ok_or_else(|| {
        anyhow!(
            "materialized view '{}' is missing schema metadata",
            params.mv_name
        )
    })?;

    let (tx, rx) = mpsc::channel(tail_stream_channel_capacity());
    let mv_for_task = Arc::clone(&mv);
    let schema_for_task = Arc::clone(&base_schema);
    let params_for_task = params.clone();

    let cancel_task = cancel.clone();
    tokio::spawn(async move {
        let mut sender = tx;
        if let Err(err) = run_tail_task(
            mv_for_task,
            schema_for_task,
            params_for_task,
            cancel_task,
            &mut sender,
        )
        .await
        {
            let _ = sender.send(Err(err)).await;
        }
    });

    Ok(TailStream::new(output_schema, rx))
}

pub fn parse_tail_sql(sql: &str) -> PgResult<TailParams> {
    match parse_floe_statement(sql)? {
        FloeStatement::Tail {
            mv_name,
            with_snapshot,
            as_of,
        } => Ok(TailParams {
            mv_name,
            with_snapshot,
            as_of,
        }),
        other => bail!("unexpected statement parsed as {other:?}"),
    }
}

pub fn tail_output_schema<C>(catalog: &C, mv_name: &str) -> PgResult<SchemaRef>
where
    C: MvCatalog + ?Sized,
{
    let base = catalog
        .schema(mv_name)
        .ok_or_else(|| anyhow!("materialized view '{}' is missing schema metadata", mv_name))?;
    Ok(build_tail_schema(&base))
}

async fn run_tail_task<M: MaterializedView + 'static>(
    mv: Arc<M>,
    schema: SchemaRef,
    params: TailParams,
    cancel: CancellationToken,
    tx: &mut mpsc::Sender<PgResult<TailBatch>>,
) -> PgResult<()> {
    let TailParams {
        mv_name,
        with_snapshot,
        as_of,
    } = params;
    let mut version_rx = mv.subscribe_versions();
    let latest = mv.latest_version();
    let mut last_emitted;
    let max_catchup_versions = tail_max_catchup_versions();

    if let Some(as_of_version) = as_of {
        ensure!(
            mv_version_exists(mv.as_ref(), as_of_version),
            "version {as_of_version} not found for requested materialized view"
        );
        if with_snapshot {
            emit_version(mv.as_ref(), &schema, &mv_name, as_of_version, tx).await?;
        }
        last_emitted = as_of_version;
    } else if with_snapshot {
        if let Some(version) = latest {
            emit_version(mv.as_ref(), &schema, &mv_name, version, tx).await?;
            last_emitted = version;
        } else {
            last_emitted = -1;
        }
    } else {
        last_emitted = latest.unwrap_or(-1);
    }

    loop {
        let latest_now = mv.latest_version().unwrap_or(last_emitted);
        if latest_now > last_emitted {
            let mut emitted = 0_i64;
            while emitted < max_catchup_versions {
                let Some(next_version) = mv.next_version_after(last_emitted) else {
                    break;
                };
                if next_version > latest_now {
                    break;
                }
                emit_delta(mv.as_ref(), &schema, &mv_name, next_version, tx).await?;
                last_emitted = next_version;
                emitted += 1;
            }
            if mv
                .next_version_after(last_emitted)
                .is_some_and(|next_version| next_version <= latest_now)
            {
                tokio::task::yield_now().await;
            }
            continue;
        }
        tokio::select! {
            _ = cancel.cancelled() => {
                return Err(query_canceled_error());
            }
            changed = version_rx.changed() => {
                if changed.is_err() {
                    break;
                }
            }
        }
    }

    Ok(())
}

fn mv_version_exists<M: MaterializedView>(mv: &M, version: i64) -> bool {
    mv.latest_version() == Some(version)
        || mv.next_version_after(version.saturating_sub(1)) == Some(version)
}

fn tail_stream_channel_capacity() -> usize {
    std::env::var("FLOE_TAIL_CHANNEL_CAPACITY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(TAIL_STREAM_CHANNEL_CAPACITY_DEFAULT)
}

fn tail_max_catchup_versions() -> i64 {
    std::env::var("FLOE_TAIL_MAX_CATCHUP_VERSIONS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(TAIL_MAX_CATCHUP_VERSIONS_DEFAULT)
}

async fn emit_version<M: MaterializedView>(
    mv: &M,
    schema: &SchemaRef,
    mv_name: &str,
    version: i64,
    tx: &mut mpsc::Sender<PgResult<TailBatch>>,
) -> PgResult<()> {
    let version_time = mv.version_time(version);
    let batches =
        materialize_snapshot_batches(mv, Arc::clone(schema), version, version_time).await?;
    let emit_span = tracing::debug_span!(
        "tail_emit",
        mv = %mv_name,
        version,
        mode = "snapshot"
    );
    let _emit_guard = emit_span.enter();
    for batch in batches {
        let payload = TailBatch { version, ..batch };
        let row_count = payload.batch.num_rows();
        if tx.send(Ok(payload)).await.is_err() {
            break;
        }
        metrics::inc_tail_rows(row_count);
        tracing::debug!(rows = row_count, "tail batch emitted");
    }
    Ok(())
}

async fn emit_delta<M: MaterializedView>(
    mv: &M,
    schema: &SchemaRef,
    mv_name: &str,
    version: i64,
    tx: &mut mpsc::Sender<PgResult<TailBatch>>,
) -> PgResult<()> {
    let delta_start = Instant::now();
    let version_time = mv.version_time(version);
    let materialize_start = Instant::now();
    let batches = materialize_delta_batches(mv, Arc::clone(schema), version, version_time).await?;
    let materialize_ms = materialize_start.elapsed().as_millis() as u64;
    let row_count: usize = batches.iter().map(|batch| batch.batch.num_rows()).sum();
    let emit_span = tracing::debug_span!(
        "tail_emit",
        mv = %mv_name,
        version,
        mode = "delta"
    );
    let _emit_guard = emit_span.enter();
    for batch in batches {
        let payload = TailBatch { version, ..batch };
        let row_count = payload.batch.num_rows();
        if tx.send(Ok(payload)).await.is_err() {
            break;
        }
        metrics::inc_tail_rows(row_count);
        tracing::debug!(rows = row_count, "tail batch emitted");
    }
    let total_ms = delta_start.elapsed().as_millis() as u64;
    let seq = TAIL_DELTA_LOG_COUNTER.fetch_add(1, Ordering::Relaxed);
    if seq < 16 || seq.is_multiple_of(TAIL_DELTA_LOG_SAMPLE_EVERY) {
        tracing::info!(
            mv = %mv_name,
            version,
            rows = row_count,
            materialize_ms,
            total_ms,
            "tail delta emit metrics"
        );
    }
    Ok(())
}

async fn materialize_snapshot_batches<M: MaterializedView>(
    mv: &M,
    schema: SchemaRef,
    version: i64,
    version_time: Option<i64>,
) -> PgResult<Vec<TailBatch>> {
    let snapshot = mv.snapshot_for(version).await?;
    let rows = rows_from_snapshot(snapshot, &schema)?;
    build_tail_batches(rows, schema, version_time)
}

async fn materialize_delta_batches<M: MaterializedView>(
    mv: &M,
    schema: SchemaRef,
    version: i64,
    version_time: Option<i64>,
) -> PgResult<Vec<TailBatch>> {
    let total_start = Instant::now();
    let delta_iter_start = Instant::now();
    let deltas = mv.delta_for(version).await?;
    let delta_iter_ms = delta_iter_start.elapsed().as_millis() as u64;
    let rows_decode_start = Instant::now();
    let rows = rows_from_delta(deltas, &schema)?;
    let rows_decode_ms = rows_decode_start.elapsed().as_millis() as u64;
    let rows_len = rows.ops.len();
    let batch_build_start = Instant::now();
    let batches = build_tail_batches(rows, schema, version_time)?;
    let batch_build_ms = batch_build_start.elapsed().as_millis() as u64;
    let total_ms = total_start.elapsed().as_millis() as u64;
    if version <= 8 || total_ms >= 1000 {
        tracing::info!(
            version,
            rows = rows_len,
            delta_iter_ms,
            rows_decode_ms,
            batch_build_ms,
            total_ms,
            "tail delta materialize breakdown"
        );
    }
    Ok(batches)
}

struct TailDecodedRows {
    builders: Vec<ScalarColumnBuilder>,
    ops: Vec<i16>,
}

fn rows_from_snapshot(
    snapshot: HashMap<Vec<u8>, i64>,
    schema: &SchemaRef,
) -> PgResult<TailDecodedRows> {
    let column_count = schema.fields().len();
    let builders = schema
        .fields()
        .iter()
        .map(|field| ScalarColumnBuilder::new(field.data_type(), 1024))
        .collect::<Result<Vec<_>>>()?;
    let mut decoded_rows = TailDecodedRows {
        builders,
        ops: Vec::new(),
    };
    for (key, diff) in snapshot {
        if diff < 0 {
            bail!("materialized view snapshot contains negative diff {diff}");
        }
        if diff == 0 {
            continue;
        }
        let decoded = decode_all_encoded_row_scalars(&key)?;
        if decoded.len() != column_count {
            bail!(
                "decoded row has {} columns but schema has {}",
                decoded.len(),
                column_count
            );
        }
        let count = diff.checked_abs().context("snapshot diff overflow")? as usize;
        for (idx, value) in decoded.iter().enumerate() {
            decoded_rows.builders[idx].append_encoded_scalar_repeated(value.as_ref(), count)?;
        }
        decoded_rows.ops.resize(decoded_rows.ops.len() + count, 1);
    }
    Ok(decoded_rows)
}

fn rows_from_delta(deltas: Vec<(Vec<u8>, i64)>, schema: &SchemaRef) -> PgResult<TailDecodedRows> {
    let column_count = schema.fields().len();
    let builders = schema
        .fields()
        .iter()
        .map(|field| ScalarColumnBuilder::new(field.data_type(), 1024))
        .collect::<Result<Vec<_>>>()?;
    let mut decoded_rows = TailDecodedRows {
        builders,
        ops: Vec::new(),
    };
    for (key, diff) in deltas {
        if diff == 0 {
            continue;
        }
        let op = if diff > 0 { 1 } else { -1 };
        let count = diff.checked_abs().context("delta diff overflow")? as usize;
        let decoded = decode_all_encoded_row_scalars(&key)?;
        if decoded.len() != column_count {
            bail!(
                "decoded row has {} columns but schema has {}",
                decoded.len(),
                column_count
            );
        }
        for (idx, value) in decoded.iter().enumerate() {
            decoded_rows.builders[idx].append_encoded_scalar_repeated(value.as_ref(), count)?;
        }
        decoded_rows.ops.resize(decoded_rows.ops.len() + count, op);
    }
    Ok(decoded_rows)
}

fn build_tail_batches(
    rows: TailDecodedRows,
    schema: SchemaRef,
    version_time: Option<i64>,
) -> PgResult<Vec<TailBatch>> {
    if rows.ops.is_empty() {
        return Ok(vec![TailBatch {
            version: 0,
            batch: RecordBatch::new_empty(schema),
            ops: Vec::new(),
            times: Vec::new(),
        }]);
    }
    let arrays = rows
        .builders
        .into_iter()
        .map(|mut builder| builder.finish_array())
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(schema, arrays)?;
    ensure!(
        rows.ops.len() == batch.num_rows(),
        "tail ops length mismatch"
    );
    Ok(vec![TailBatch {
        version: 0,
        times: vec![version_time; batch.num_rows()],
        batch,
        ops: rows.ops,
    }])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dbsp_bridge::DbspBridge;
    use crate::materialized_view::DbspPersistedState;
    use crate::mv::registry::MaterializedViewRegistry;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use dbsp::StreamRetention;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use tokio::time::{Duration, timeout};

    fn build_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]))
    }

    fn encoded_i64_row(value: i64) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(4 + 1 + 8);
        encoded.extend_from_slice(&(1_u32).to_le_bytes());
        encoded.push(0x01);
        encoded.extend_from_slice(&value.to_le_bytes());
        encoded
    }

    async fn append_version(
        view: &mut crate::dbsp_bridge::DbspView,
        values: &[i64],
    ) -> PgResult<dbsp::handles::ZSetHandle> {
        view.add_deltas(
            values
                .iter()
                .copied()
                .map(|value| (encoded_i64_row(value), 1)),
        );
        view.flush().await
    }

    async fn append_deltas(
        view: &mut crate::dbsp_bridge::DbspView,
        deltas: &[(i64, i64)],
    ) -> PgResult<dbsp::handles::ZSetHandle> {
        view.add_deltas(
            deltas
                .iter()
                .copied()
                .map(|(value, diff)| (encoded_i64_row(value), diff)),
        );
        view.flush().await
    }

    #[tokio::test]
    async fn snapshot_then_streams_new_versions() -> PgResult<()> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("tail-test", store).await.expect("db"));
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let mut dbsp_view = bridge
            .new_view("mv_tail", StreamRetention::KeepLast { keep_last: 1 })
            .await?;

        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_tail", build_schema());
        let handle = registry.register("mv_tail");

        let handle1 = append_version(&mut dbsp_view, &[1]).await?;
        let latest_view = dbsp_view.latest_handle_view();
        let (dict, table, ns, version) = latest_view.into_parts();
        let state = DbspPersistedState::new(dict, table, ns, version);
        handle.set_dbsp_state(state);
        handle.publish_version(handle1.version as i64, handle1);

        let handle2 = append_version(&mut dbsp_view, &[2]).await?;
        let handle2_version = handle2.version as i64;
        handle.publish_version(handle2_version, handle2);

        let ctx = SessionContext::new();
        let params = TailParams {
            mv_name: "mv_tail".to_string(),
            with_snapshot: true,
            as_of: None,
        };

        let cancel = CancellationToken::new();
        let mut stream = execute_tail(&ctx, registry.as_ref(), params, cancel.clone()).await?;
        let first_batch = stream.next().await.expect("expected initial batch")?;
        assert_eq!(first_batch.version, handle2_version);
        let values = first_batch
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int column");
        let first_snapshot: Vec<i64> = (0..values.len()).map(|idx| values.value(idx)).collect();
        assert!(first_snapshot.contains(&2));

        let handle3 = append_version(&mut dbsp_view, &[3]).await?;
        let handle3_version = handle3.version as i64;
        handle.publish_version(handle3_version, handle3);

        let second_batch = stream.next().await.expect("expected update batch")?;
        assert_eq!(second_batch.version, handle3_version);
        let values = second_batch
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int column");
        let second_snapshot: Vec<i64> = (0..values.len()).map(|idx| values.value(idx)).collect();
        assert!(second_snapshot.contains(&3));

        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn tail_emits_delta_ops() -> PgResult<()> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("tail-delta-ops", store).await.expect("db"));
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let mut dbsp_view = bridge
            .new_view(
                "mv_tail_delta_ops",
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await?;

        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_tail_delta_ops", build_schema());
        let handle = registry.register("mv_tail_delta_ops");

        let handle1 = append_deltas(&mut dbsp_view, &[(1, 1)]).await?;
        let latest_view = dbsp_view.latest_handle_view();
        let (dict, table, ns, version) = latest_view.into_parts();
        let state = DbspPersistedState::new(dict, table, ns, version);
        handle.set_dbsp_state(state);
        let handle1_version = handle1.version as i64;
        handle.publish_version(handle1_version, handle1);

        let ctx = SessionContext::new();
        let params = TailParams {
            mv_name: "mv_tail_delta_ops".to_string(),
            with_snapshot: false,
            as_of: Some(handle1_version),
        };
        let cancel = CancellationToken::new();
        let mut stream = execute_tail(&ctx, registry.as_ref(), params, cancel.clone()).await?;

        let handle2 = append_deltas(&mut dbsp_view, &[(1, -1), (2, 1)]).await?;
        let handle2_version = handle2.version as i64;
        handle.publish_version(handle2_version, handle2);

        let batch = timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("timeout waiting for delta batch")
            .expect("expected delta batch")?;
        assert_eq!(batch.version, handle2_version);
        let values = batch
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int column");
        assert_eq!(batch.ops.len(), values.len());
        assert_eq!(batch.times.len(), values.len());
        assert!(batch.times.iter().all(|time| time.is_some()));
        let mut rows = Vec::new();
        for idx in 0..values.len() {
            rows.push((values.value(idx), batch.ops[idx]));
        }
        assert!(rows.contains(&(1, -1)));
        assert!(rows.contains(&(2, 1)));
        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn tail_emits_empty_batch_for_published_noop_version() -> PgResult<()> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("tail-empty-version", store).await.expect("db"));
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let mut dbsp_view = bridge
            .new_view(
                "mv_tail_empty_version",
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await?;

        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_tail_empty_version", build_schema());
        let handle = registry.register("mv_tail_empty_version");

        let handle1 = append_deltas(&mut dbsp_view, &[(1, 1)]).await?;
        let latest_view = dbsp_view.latest_handle_view();
        let (dict, table, ns, version) = latest_view.into_parts();
        let state = DbspPersistedState::new(dict, table, ns, version).with_logical_version(2);
        handle.set_dbsp_state(state);
        let handle1_version = handle1.version as i64;
        handle.publish_version(handle1_version, handle1);

        let ctx = SessionContext::new();
        let params = TailParams {
            mv_name: "mv_tail_empty_version".to_string(),
            with_snapshot: false,
            as_of: Some(handle1_version),
        };
        let cancel = CancellationToken::new();
        let mut stream = execute_tail(&ctx, registry.as_ref(), params, cancel.clone()).await?;

        handle.publish_logical_version(2);

        let batch = timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("timeout waiting for empty tail batch")
            .expect("expected empty tail batch")?;
        assert_eq!(batch.version, 2);
        assert_eq!(batch.batch.num_rows(), 0);
        assert!(batch.ops.is_empty());
        assert!(batch.times.is_empty());

        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn tail_deltas_match_materialized_state() -> PgResult<()> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("tail-delta-validate", store).await.expect("db"));
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let mut dbsp_view = bridge
            .new_view(
                "mv_tail_delta_validate",
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await?;

        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_tail_delta_validate", build_schema());
        let handle = registry.register("mv_tail_delta_validate");

        let handle1 = append_deltas(&mut dbsp_view, &[(1, 1), (2, 1)]).await?;
        let latest_view = dbsp_view.latest_handle_view();
        let (dict, table, ns, version) = latest_view.into_parts();
        let state = DbspPersistedState::new(dict, table, ns, version);
        handle.set_dbsp_state(state);
        let handle1_version = handle1.version as i64;
        handle.publish_version(handle1_version, handle1);

        let ctx = SessionContext::new();
        let params = TailParams {
            mv_name: "mv_tail_delta_validate".to_string(),
            with_snapshot: true,
            as_of: Some(handle1_version),
        };
        let cancel = CancellationToken::new();
        let mut stream = execute_tail(&ctx, registry.as_ref(), params, cancel.clone()).await?;

        let handle2 = append_deltas(&mut dbsp_view, &[(1, -1), (3, 1)]).await?;
        let handle2_version = handle2.version as i64;
        handle.publish_version(handle2_version, handle2);

        let handle3 = append_deltas(&mut dbsp_view, &[(2, -1), (4, 1)]).await?;
        let handle3_version = handle3.version as i64;
        handle.publish_version(handle3_version, handle3);

        let mut state: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
        for _ in 0..3 {
            let batch = timeout(Duration::from_millis(200), stream.next())
                .await
                .expect("timeout waiting for tail batch")
                .expect("expected tail batch")?;
            let values = batch
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int column");
            for idx in 0..values.len() {
                let value = values.value(idx);
                let op = batch.ops[idx] as i64;
                let entry = state.entry(value).or_insert(0);
                *entry += op;
                if *entry == 0 {
                    state.remove(&value);
                }
            }
        }

        let view = MaterializedView::handle_for(handle.as_ref(), handle3_version)?;
        let snapshot = view.materialize().await?;
        let mut expected: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
        for (key, diff) in snapshot {
            if diff == 0 {
                continue;
            }
            let row = decode_all_encoded_row_scalars(&key)?;
            let value = match row.first().and_then(|value| value.as_ref()) {
                Some(crate::encoding::EncodedRowScalar::Int64(v)) => *v,
                _ => continue,
            };
            expected.insert(value, diff);
        }

        assert_eq!(state, expected);
        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn tail_stream_cancels() -> PgResult<()> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("tail-cancel", store).await.expect("db"));
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let mut dbsp_view = bridge
            .new_view("mv_tail_cancel", StreamRetention::KeepLast { keep_last: 1 })
            .await?;

        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_tail_cancel", build_schema());
        let handle = registry.register("mv_tail_cancel");

        let handle1 = append_version(&mut dbsp_view, &[1]).await?;
        let latest_view = dbsp_view.latest_handle_view();
        let (dict, table, ns, version) = latest_view.into_parts();
        let state = DbspPersistedState::new(dict, table, ns, version);
        handle.set_dbsp_state(state);
        handle.publish_version(handle1.version as i64, handle1);

        let ctx = SessionContext::new();
        let params = TailParams {
            mv_name: "mv_tail_cancel".to_string(),
            with_snapshot: false,
            as_of: None,
        };
        let cancel = CancellationToken::new();
        let mut stream = execute_tail(&ctx, registry.as_ref(), params, cancel.clone()).await?;
        cancel.cancel();
        let err = timeout(Duration::from_millis(100), stream.next())
            .await
            .expect("cancellation timeout")
            .expect("expected cancellation event")
            .expect_err("expected cancellation error");
        assert!(is_tail_canceled_error(&err));
        Ok(())
    }

    #[tokio::test]
    async fn as_of_with_snapshot_emits_requested_version() -> PgResult<()> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("tail-asof-snap", store).await.expect("db"));
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let mut dbsp_view = bridge
            .new_view(
                "mv_tail_asof_snap",
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await?;

        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_tail_asof_snap", build_schema());
        let handle = registry.register("mv_tail_asof_snap");

        let handle1 = append_version(&mut dbsp_view, &[10]).await?;
        let latest_view = dbsp_view.latest_handle_view();
        let (dict, table, ns, version) = latest_view.into_parts();
        let state = DbspPersistedState::new(dict, table, ns, version);
        handle.set_dbsp_state(state);
        let handle1_version = handle1.version as i64;
        handle.publish_version(handle1_version, handle1);

        let handle2 = append_version(&mut dbsp_view, &[20]).await?;
        handle.publish_version(handle2.version as i64, handle2);

        let ctx = SessionContext::new();
        let params = TailParams {
            mv_name: "mv_tail_asof_snap".to_string(),
            with_snapshot: true,
            as_of: Some(handle1_version),
        };
        let cancel = CancellationToken::new();
        let mut stream = execute_tail(&ctx, registry.as_ref(), params, cancel.clone()).await?;
        let batch = stream.next().await.expect("snapshot batch")?;
        assert_eq!(batch.version, handle1_version);
        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn as_of_with_snapshot_accepts_published_empty_logical_version() -> PgResult<()> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(
            Db::open("tail-asof-empty-logical", store)
                .await
                .expect("db"),
        );
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let mut dbsp_view = bridge
            .new_view(
                "mv_tail_asof_empty_logical",
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await?;

        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_tail_asof_empty_logical", build_schema());
        let handle = registry.register("mv_tail_asof_empty_logical");

        let handle1 = append_version(&mut dbsp_view, &[10]).await?;
        let latest_view = dbsp_view.latest_handle_view();
        let (dict, table, ns, version) = latest_view.into_parts();
        let state = DbspPersistedState::new(dict, table, ns, version).with_logical_version(2);
        handle.set_dbsp_state(state);
        handle.publish_version(handle1.version as i64, handle1);
        handle.publish_logical_version(2);

        let ctx = SessionContext::new();
        let params = TailParams {
            mv_name: "mv_tail_asof_empty_logical".to_string(),
            with_snapshot: true,
            as_of: Some(2),
        };
        let cancel = CancellationToken::new();
        let mut stream = execute_tail(&ctx, registry.as_ref(), params, cancel.clone()).await?;

        let batch = stream.next().await.expect("snapshot batch")?;
        assert_eq!(batch.version, 2);
        assert_eq!(batch.batch.num_rows(), 1);
        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn as_of_without_snapshot_starts_after_version() -> PgResult<()> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("tail-asof-no-snap", store).await.expect("db"));
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let mut dbsp_view = bridge
            .new_view(
                "mv_tail_asof_no_snap",
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await?;

        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_tail_asof_no_snap", build_schema());
        let handle = registry.register("mv_tail_asof_no_snap");

        let handle1 = append_version(&mut dbsp_view, &[1]).await?;
        let latest_view = dbsp_view.latest_handle_view();
        let (dict, table, ns, version) = latest_view.into_parts();
        let state = DbspPersistedState::new(dict, table, ns, version);
        handle.set_dbsp_state(state);
        handle.publish_version(handle1.version as i64, handle1);

        let handle2 = append_version(&mut dbsp_view, &[2]).await?;
        let handle2_version = handle2.version as i64;
        handle.publish_version(handle2_version, handle2);

        let ctx = SessionContext::new();
        let params = TailParams {
            mv_name: "mv_tail_asof_no_snap".to_string(),
            with_snapshot: false,
            as_of: Some(handle2_version),
        };
        let cancel = CancellationToken::new();
        let mut stream = execute_tail(&ctx, registry.as_ref(), params, cancel.clone()).await?;

        assert!(
            timeout(Duration::from_millis(20), stream.next())
                .await
                .is_err()
        );

        let handle3 = append_version(&mut dbsp_view, &[3]).await?;
        let handle3_version = handle3.version as i64;
        handle.publish_version(handle3_version, handle3);

        let batch = stream.next().await.expect("post-asof batch")?;
        assert_eq!(batch.version, handle3_version);
        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn with_snapshot_waits_for_first_version_when_view_starts_empty() -> PgResult<()> {
        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_tail_empty_start", build_schema());
        let handle = registry.register("mv_tail_empty_start");

        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("tail-empty-start", store).await.expect("db"));
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let mut dbsp_view = bridge
            .new_view(
                "mv_tail_empty_start",
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await?;

        let ctx = SessionContext::new();
        let params = TailParams {
            mv_name: "mv_tail_empty_start".to_string(),
            with_snapshot: true,
            as_of: None,
        };
        let cancel = CancellationToken::new();
        let mut stream = execute_tail(&ctx, registry.as_ref(), params, cancel.clone()).await?;

        assert!(
            timeout(Duration::from_millis(20), stream.next())
                .await
                .is_err()
        );

        let handle1 = append_version(&mut dbsp_view, &[11]).await?;
        let latest_view = dbsp_view.latest_handle_view();
        let (dict, table, ns, version) = latest_view.into_parts();
        let state =
            DbspPersistedState::new(dict, table, ns, version).with_logical_version(handle1.version);
        handle.set_dbsp_state(state);
        handle.publish_version(handle1.version as i64, handle1);

        let batch = timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("timeout waiting for first version")
            .expect("expected first tail batch")?;
        assert_eq!(batch.version, 1);
        cancel.cancel();
        Ok(())
    }
}
