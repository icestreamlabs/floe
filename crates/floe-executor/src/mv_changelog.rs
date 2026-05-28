use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context as TaskContext, Poll};
use std::time::Instant;

use anyhow::{Result, anyhow, ensure};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use futures::Stream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::encoded_batch::{
    EncodedRowBatchMode, ExpandedEncodedBatch, build_expanded_batches_from_encoded_rows,
};
use crate::materialized_view::{MaterializedViewHandle, MaterializedViewRegistry};
use crate::metrics;
use crate::mv::runtime::MaterializedView;

pub type MvChangelogResult<T> = Result<T>;

const MV_CHANGELOG_CHANNEL_CAPACITY_DEFAULT: usize = 256;
const MV_CHANGELOG_MAX_CATCHUP_VERSIONS_DEFAULT: i64 = 32;
const MV_CHANGELOG_DELTA_LOG_SAMPLE_EVERY: usize = 128;
static MV_CHANGELOG_DELTA_LOG_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MvChangelogExecutionConfig {
    pub channel_capacity: usize,
    pub max_catchup_versions: i64,
}

impl Default for MvChangelogExecutionConfig {
    fn default() -> Self {
        Self {
            channel_capacity: MV_CHANGELOG_CHANNEL_CAPACITY_DEFAULT,
            max_catchup_versions: MV_CHANGELOG_MAX_CATCHUP_VERSIONS_DEFAULT,
        }
    }
}

impl MvChangelogExecutionConfig {
    fn channel_capacity(self) -> usize {
        self.channel_capacity.max(1)
    }

    fn max_catchup_versions(self) -> i64 {
        self.max_catchup_versions.max(1)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MvChangelogBatchKind {
    Snapshot,
    Delta,
}

#[derive(Debug)]
pub struct MvChangelogBatch {
    pub version: i64,
    pub version_time: Option<i64>,
    pub kind: MvChangelogBatchKind,
    pub batch: RecordBatch,
    pub diffs: Vec<i64>,
}

#[derive(Debug, Clone)]
pub struct MvChangelogParams {
    pub mv_name: String,
    pub with_snapshot: bool,
    pub as_of: Option<i64>,
}

#[derive(Debug)]
pub struct MvChangelogStream {
    schema: SchemaRef,
    receiver: mpsc::Receiver<MvChangelogResult<MvChangelogBatch>>,
}

impl MvChangelogStream {
    fn new(
        schema: SchemaRef,
        receiver: mpsc::Receiver<MvChangelogResult<MvChangelogBatch>>,
    ) -> Self {
        Self { schema, receiver }
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for MvChangelogStream {
    type Item = MvChangelogResult<MvChangelogBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_recv(cx)
    }
}

#[derive(Debug)]
struct MvChangelogCanceledError;

impl fmt::Display for MvChangelogCanceledError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "query canceled")
    }
}

impl std::error::Error for MvChangelogCanceledError {}

fn query_canceled_error() -> anyhow::Error {
    anyhow::Error::new(MvChangelogCanceledError)
}

pub fn is_mv_changelog_canceled_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<MvChangelogCanceledError>().is_some()
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

pub async fn execute_mv_changelog<C>(
    catalog: &C,
    params: MvChangelogParams,
    cancel: CancellationToken,
) -> MvChangelogResult<MvChangelogStream>
where
    C: MvCatalog + ?Sized,
{
    execute_mv_changelog_with_config(
        catalog,
        params,
        MvChangelogExecutionConfig::default(),
        cancel,
    )
    .await
}

pub async fn execute_mv_changelog_with_config<C>(
    catalog: &C,
    params: MvChangelogParams,
    config: MvChangelogExecutionConfig,
    cancel: CancellationToken,
) -> MvChangelogResult<MvChangelogStream>
where
    C: MvCatalog + ?Sized,
{
    crate::metrics::init();
    let mv = catalog
        .materialized_view(&params.mv_name)
        .ok_or_else(|| anyhow!("materialized view '{}' not found", params.mv_name))?;
    let schema = catalog.schema(&params.mv_name).ok_or_else(|| {
        anyhow!(
            "materialized view '{}' is missing schema metadata",
            params.mv_name
        )
    })?;

    let (tx, rx) = mpsc::channel(config.channel_capacity());
    let mv_for_task = Arc::clone(&mv);
    let schema_for_task = Arc::clone(&schema);
    let params_for_task = params.clone();

    let cancel_task = cancel.clone();
    tokio::spawn(async move {
        let mut sender = tx;
        if let Err(err) = run_mv_changelog_task(
            mv_for_task,
            schema_for_task,
            params_for_task,
            config,
            cancel_task,
            &mut sender,
        )
        .await
        {
            let _ = sender.send(Err(err)).await;
        }
    });

    Ok(MvChangelogStream::new(schema, rx))
}

async fn run_mv_changelog_task<M: MaterializedView + 'static>(
    mv: Arc<M>,
    schema: SchemaRef,
    params: MvChangelogParams,
    config: MvChangelogExecutionConfig,
    cancel: CancellationToken,
    tx: &mut mpsc::Sender<MvChangelogResult<MvChangelogBatch>>,
) -> MvChangelogResult<()> {
    let MvChangelogParams {
        mv_name,
        with_snapshot,
        as_of,
    } = params;
    let mut version_rx = mv.subscribe_versions();
    let latest = mv.latest_version();
    let mut last_emitted;
    let max_catchup_versions = config.max_catchup_versions();

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

async fn emit_version<M: MaterializedView>(
    mv: &M,
    schema: &SchemaRef,
    mv_name: &str,
    version: i64,
    tx: &mut mpsc::Sender<MvChangelogResult<MvChangelogBatch>>,
) -> MvChangelogResult<()> {
    let version_time = mv.version_time(version);
    let batches =
        materialize_snapshot_batches(mv, Arc::clone(schema), version, version_time).await?;
    let emit_span = tracing::debug_span!(
        "mv_changelog_emit",
        mv = %mv_name,
        version,
        mode = "snapshot"
    );
    let _emit_guard = emit_span.enter();
    for batch in batches {
        let row_count = batch.batch.num_rows();
        if tx.send(Ok(batch)).await.is_err() {
            break;
        }
        metrics::inc_tail_rows(row_count);
        tracing::debug!(rows = row_count, "mv changelog batch emitted");
    }
    Ok(())
}

async fn emit_delta<M: MaterializedView>(
    mv: &M,
    schema: &SchemaRef,
    mv_name: &str,
    version: i64,
    tx: &mut mpsc::Sender<MvChangelogResult<MvChangelogBatch>>,
) -> MvChangelogResult<()> {
    let delta_start = Instant::now();
    let version_time = mv.version_time(version);
    let materialize_start = Instant::now();
    let batches = materialize_delta_batches(mv, Arc::clone(schema), version, version_time).await?;
    let materialize_ms = materialize_start.elapsed().as_millis() as u64;
    let row_count: usize = batches.iter().map(|batch| batch.batch.num_rows()).sum();
    let emit_span = tracing::debug_span!(
        "mv_changelog_emit",
        mv = %mv_name,
        version,
        mode = "delta"
    );
    let _emit_guard = emit_span.enter();
    for batch in batches {
        let row_count = batch.batch.num_rows();
        if tx.send(Ok(batch)).await.is_err() {
            break;
        }
        metrics::inc_tail_rows(row_count);
        tracing::debug!(rows = row_count, "mv changelog batch emitted");
    }
    let total_ms = delta_start.elapsed().as_millis() as u64;
    let seq = MV_CHANGELOG_DELTA_LOG_COUNTER.fetch_add(1, Ordering::Relaxed);
    if seq < 16 || seq.is_multiple_of(MV_CHANGELOG_DELTA_LOG_SAMPLE_EVERY) {
        tracing::info!(
            mv = %mv_name,
            version,
            rows = row_count,
            materialize_ms,
            total_ms,
            "mv changelog delta emit metrics"
        );
    }
    Ok(())
}

async fn materialize_snapshot_batches<M: MaterializedView>(
    mv: &M,
    schema: SchemaRef,
    version: i64,
    version_time: Option<i64>,
) -> MvChangelogResult<Vec<MvChangelogBatch>> {
    let snapshot = mv.snapshot_for(version).await?;
    let (_schema, rows) = build_expanded_batches_from_encoded_rows(
        snapshot,
        schema,
        None,
        None,
        None,
        EncodedRowBatchMode::Snapshot,
    )?;
    Ok(build_mv_changelog_batches(
        rows,
        version,
        version_time,
        MvChangelogBatchKind::Snapshot,
    ))
}

async fn materialize_delta_batches<M: MaterializedView>(
    mv: &M,
    schema: SchemaRef,
    version: i64,
    version_time: Option<i64>,
) -> MvChangelogResult<Vec<MvChangelogBatch>> {
    let total_start = Instant::now();
    let delta_iter_start = Instant::now();
    let deltas = mv.delta_for(version).await?;
    let delta_iter_ms = delta_iter_start.elapsed().as_millis() as u64;
    let rows_decode_start = Instant::now();
    let (_schema, rows) = build_expanded_batches_from_encoded_rows(
        deltas,
        schema,
        None,
        None,
        None,
        EncodedRowBatchMode::Delta,
    )?;
    let rows_decode_ms = rows_decode_start.elapsed().as_millis() as u64;
    let rows_len = rows.iter().map(|row| row.diffs.len()).sum::<usize>();
    let batch_build_start = Instant::now();
    let batches =
        build_mv_changelog_batches(rows, version, version_time, MvChangelogBatchKind::Delta);
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
            "mv changelog delta materialize breakdown"
        );
    }
    Ok(batches)
}

fn build_mv_changelog_batches(
    rows: Vec<ExpandedEncodedBatch>,
    version: i64,
    version_time: Option<i64>,
    kind: MvChangelogBatchKind,
) -> Vec<MvChangelogBatch> {
    rows.into_iter()
        .map(|row| {
            debug_assert_eq!(row.diffs.len(), row.batch.num_rows());
            MvChangelogBatch {
                version,
                version_time,
                kind,
                batch: row.batch,
                diffs: row.diffs,
            }
        })
        .collect()
}
