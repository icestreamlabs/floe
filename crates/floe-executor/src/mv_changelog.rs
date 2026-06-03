use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context as TaskContext, Poll};
use std::time::Instant;

use anyhow::{Result, anyhow, ensure};
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use futures::Stream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::encoded_batch::{
    encoded_deltas_to_weighted_arrow_batches, encoded_snapshot_to_arrow_batches,
};
use crate::metrics;
use crate::mv::registry::{MaterializedViewHandle, MaterializedViewRegistry};
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
        metrics::inc_subscribe_rows(row_count);
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
        metrics::inc_subscribe_rows(row_count);
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
    if let Some(snapshot) = mv.arrow_snapshot_for(version) {
        return arrow_snapshot_batches_to_changelog(snapshot, schema, version, version_time);
    }
    let snapshot = mv.snapshot_for(version).await?;
    let batches = encoded_snapshot_to_arrow_batches(&snapshot, Arc::clone(&schema), None)?;
    snapshot_batches_to_changelog(batches, schema, version, version_time)
}

async fn materialize_delta_batches<M: MaterializedView>(
    mv: &M,
    schema: SchemaRef,
    version: i64,
    version_time: Option<i64>,
) -> MvChangelogResult<Vec<MvChangelogBatch>> {
    let total_start = Instant::now();
    if let Some(delta) = mv.arrow_delta_for(version) {
        return arrow_delta_batches_to_changelog(delta, schema, version, version_time);
    }
    let delta = mv.delta_for(version).await?;
    let batches = encoded_deltas_to_weighted_arrow_batches(&delta, Arc::clone(&schema))?;
    tracing::debug!(
        version,
        rows = delta.len(),
        total_ms = total_start.elapsed().as_millis() as u64,
        "mv changelog materialized encoded delta"
    );
    arrow_delta_batches_to_changelog(Arc::new(batches), schema, version, version_time)
}

fn arrow_snapshot_batches_to_changelog(
    snapshot: Arc<Vec<RecordBatch>>,
    schema: SchemaRef,
    version: i64,
    version_time: Option<i64>,
) -> MvChangelogResult<Vec<MvChangelogBatch>> {
    snapshot_batches_to_changelog(
        snapshot.iter().cloned().collect(),
        schema,
        version,
        version_time,
    )
}

fn snapshot_batches_to_changelog(
    snapshot: Vec<RecordBatch>,
    schema: SchemaRef,
    version: i64,
    version_time: Option<i64>,
) -> MvChangelogResult<Vec<MvChangelogBatch>> {
    let mut batches = Vec::new();
    for batch in snapshot {
        ensure!(
            batch.schema().as_ref() == schema.as_ref(),
            "Arrow MV snapshot schema does not match catalog schema"
        );
        let row_count = batch.num_rows();
        batches.push(MvChangelogBatch {
            version,
            version_time,
            kind: MvChangelogBatchKind::Snapshot,
            batch,
            diffs: vec![1; row_count],
        });
    }
    if batches.is_empty() {
        batches.push(MvChangelogBatch {
            version,
            version_time,
            kind: MvChangelogBatchKind::Snapshot,
            batch: RecordBatch::new_empty(schema),
            diffs: Vec::new(),
        });
    }
    Ok(batches)
}

fn arrow_delta_batches_to_changelog(
    delta: Arc<Vec<RecordBatch>>,
    schema: SchemaRef,
    version: i64,
    version_time: Option<i64>,
) -> MvChangelogResult<Vec<MvChangelogBatch>> {
    let mut batches = Vec::new();
    for batch in delta.iter() {
        if batch.num_rows() == 0 {
            continue;
        }
        let weight_idx = batch.schema().index_of(WEIGHT_COLUMN_NAME)?;
        let weight_col = batch
            .column(weight_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow!("Arrow MV delta {} column must be Int64", WEIGHT_COLUMN_NAME))?;
        ensure!(
            weight_col.null_count() == 0,
            "Arrow MV delta {} column cannot contain NULL",
            WEIGHT_COLUMN_NAME
        );
        let diffs = weight_col.values().to_vec();
        let columns = batch
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(idx, column)| (idx != weight_idx).then_some(Arc::clone(column)))
            .collect::<Vec<_>>();
        let projected = RecordBatch::try_new(Arc::clone(&schema), columns)?;
        batches.push(MvChangelogBatch {
            version,
            version_time,
            kind: MvChangelogBatchKind::Delta,
            batch: projected,
            diffs,
        });
    }
    if batches.is_empty() {
        batches.push(MvChangelogBatch {
            version,
            version_time,
            kind: MvChangelogBatchKind::Delta,
            batch: RecordBatch::new_empty(schema),
            diffs: Vec::new(),
        });
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::mv::registry::{MaterializedViewHandle, MaterializedViewRegistry};
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;
    use tokio::time::{Duration, timeout};
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn build_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]))
    }

    fn value_batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            build_schema(),
            vec![Arc::new(Int64Array::from_iter_values(
                values.iter().copied(),
            ))],
        )
        .expect("build Arrow value batch")
    }

    fn delta_batch(deltas: &[(i64, i64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN_NAME, DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(
                    deltas.iter().map(|(value, _)| *value),
                )),
                Arc::new(Int64Array::from_iter_values(
                    deltas.iter().map(|(_, diff)| *diff),
                )),
            ],
        )
        .expect("build Arrow delta batch")
    }

    fn publish_arrow_version(
        handle: &MaterializedViewHandle,
        version: i64,
        snapshot_values: &[i64],
        deltas: &[(i64, i64)],
    ) {
        let delta = if deltas.is_empty() {
            Vec::new()
        } else {
            vec![delta_batch(deltas)]
        };
        handle.publish_arrow_version(version, vec![value_batch(snapshot_values)], delta);
    }

    fn encoded_i64_row(value: i64) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(4 + 1 + 8);
        encoded.extend_from_slice(&(1_u32).to_le_bytes());
        encoded.push(0x01);
        encoded.extend_from_slice(&value.to_le_bytes());
        encoded
    }

    fn publish_encoded_overlay(
        handle: &MaterializedViewHandle,
        version: u64,
        deltas: &[(i64, i64)],
    ) {
        handle.append_encoded_overlay_batch(
            version,
            deltas
                .iter()
                .map(|(value, diff)| (encoded_i64_row(*value), *diff)),
        );
    }

    fn batch_values(batch: &MvChangelogBatch) -> Vec<i64> {
        let values = batch
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int column");
        (0..values.len()).map(|idx| values.value(idx)).collect()
    }

    fn batch_rows_with_diffs(batch: &MvChangelogBatch) -> Vec<(i64, i64)> {
        batch_values(batch)
            .into_iter()
            .zip(batch.diffs.iter().copied())
            .collect()
    }

    #[tokio::test]
    async fn snapshot_then_streams_new_versions() -> MvChangelogResult<()> {
        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_subscribe", build_schema());
        let handle = registry.register("mv_subscribe");

        publish_arrow_version(handle.as_ref(), 1, &[1], &[(1, 1)]);
        let handle2_version = 2_i64;
        publish_arrow_version(handle.as_ref(), handle2_version, &[1, 2], &[(2, 1)]);

        let params = MvChangelogParams {
            mv_name: "mv_subscribe".to_string(),
            with_snapshot: true,
            as_of: None,
        };

        let cancel = CancellationToken::new();
        let mut stream = execute_mv_changelog(registry.as_ref(), params, cancel.clone()).await?;
        let first_batch = stream.next().await.expect("expected initial batch")?;
        assert_eq!(first_batch.version, handle2_version);
        assert_eq!(first_batch.kind, MvChangelogBatchKind::Snapshot);
        assert!(batch_values(&first_batch).contains(&2));

        let handle3_version = 3_i64;
        publish_arrow_version(handle.as_ref(), handle3_version, &[1, 2, 3], &[(3, 1)]);

        let second_batch = stream.next().await.expect("expected update batch")?;
        assert_eq!(second_batch.version, handle3_version);
        assert_eq!(second_batch.kind, MvChangelogBatchKind::Delta);
        assert!(batch_values(&second_batch).contains(&3));

        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn changelog_emits_delta_diffs() -> MvChangelogResult<()> {
        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_subscribe_delta_diffs", build_schema());
        let handle = registry.register("mv_subscribe_delta_diffs");

        let handle1_version = 1_i64;
        publish_arrow_version(handle.as_ref(), handle1_version, &[1], &[(1, 1)]);

        let params = MvChangelogParams {
            mv_name: "mv_subscribe_delta_diffs".to_string(),
            with_snapshot: false,
            as_of: Some(handle1_version),
        };
        let cancel = CancellationToken::new();
        let mut stream = execute_mv_changelog(registry.as_ref(), params, cancel.clone()).await?;

        let handle2_version = 2_i64;
        publish_arrow_version(handle.as_ref(), handle2_version, &[2], &[(1, -1), (2, 1)]);

        let batch = timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("timeout waiting for delta batch")
            .expect("expected delta batch")?;
        assert_eq!(batch.version, handle2_version);
        assert_eq!(batch.kind, MvChangelogBatchKind::Delta);
        assert!(batch.version_time.is_some());
        let rows = batch_rows_with_diffs(&batch);
        assert!(rows.contains(&(1, -1)));
        assert!(rows.contains(&(2, 1)));
        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_uses_encoded_overlay_when_arrow_snapshot_is_missing() -> MvChangelogResult<()>
    {
        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_encoded_snapshot", build_schema());
        let handle = registry.register("mv_encoded_snapshot");
        publish_encoded_overlay(handle.as_ref(), 1, &[(10, 1), (20, 1)]);

        let params = MvChangelogParams {
            mv_name: "mv_encoded_snapshot".to_string(),
            with_snapshot: true,
            as_of: Some(1),
        };
        let cancel = CancellationToken::new();
        let mut stream = execute_mv_changelog(registry.as_ref(), params, cancel.clone()).await?;
        let batch = stream.next().await.expect("snapshot batch")?;

        assert_eq!(batch.kind, MvChangelogBatchKind::Snapshot);
        assert_eq!(batch.version, 1);
        let mut rows = batch_values(&batch);
        rows.sort_unstable();
        assert_eq!(rows, vec![10, 20]);
        assert_eq!(batch.diffs, vec![1, 1]);
        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn delta_uses_encoded_overlay_when_arrow_delta_is_missing() -> MvChangelogResult<()> {
        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_encoded_delta", build_schema());
        let handle = registry.register("mv_encoded_delta");
        publish_encoded_overlay(handle.as_ref(), 1, &[(10, 1), (20, 1)]);

        let params = MvChangelogParams {
            mv_name: "mv_encoded_delta".to_string(),
            with_snapshot: false,
            as_of: Some(1),
        };
        let cancel = CancellationToken::new();
        let mut stream = execute_mv_changelog(registry.as_ref(), params, cancel.clone()).await?;
        publish_encoded_overlay(handle.as_ref(), 2, &[(10, -1), (30, 1)]);

        let batch = timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("timeout waiting for encoded delta")
            .expect("encoded delta batch")?;
        assert_eq!(batch.kind, MvChangelogBatchKind::Delta);
        let mut rows = batch_rows_with_diffs(&batch);
        rows.sort_unstable();
        assert_eq!(rows, vec![(10, -1), (30, 1)]);
        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn delta_output_is_bounded_by_delta_not_snapshot_size() -> MvChangelogResult<()> {
        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_subscribe_delta_bounded", build_schema());
        let handle = registry.register("mv_subscribe_delta_bounded");

        let initial_values = (0..100).collect::<Vec<_>>();
        let handle1_version = 1_i64;
        publish_arrow_version(handle.as_ref(), handle1_version, &initial_values, &[]);

        let params = MvChangelogParams {
            mv_name: "mv_subscribe_delta_bounded".to_string(),
            with_snapshot: false,
            as_of: Some(handle1_version),
        };
        let cancel = CancellationToken::new();
        let mut stream = execute_mv_changelog(registry.as_ref(), params, cancel.clone()).await?;

        let handle2_version = 2_i64;
        let mut next_snapshot = initial_values
            .iter()
            .copied()
            .filter(|value| *value != 1)
            .collect::<Vec<_>>();
        next_snapshot.push(101);
        publish_arrow_version(
            handle.as_ref(),
            handle2_version,
            &next_snapshot,
            &[(1, -1), (101, 1)],
        );

        let batch = timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("timeout waiting for bounded delta batch")
            .expect("expected delta batch")?;
        assert_eq!(batch.version, handle2_version);
        assert_eq!(batch.batch.num_rows(), 2);
        assert_eq!(batch.diffs.len(), 2);
        assert!(batch.diffs.contains(&-1));
        assert!(batch.diffs.contains(&1));

        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn emits_empty_batch_for_published_noop_version() -> MvChangelogResult<()> {
        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_subscribe_empty_version", build_schema());
        let handle = registry.register("mv_subscribe_empty_version");

        let handle1_version = 1_i64;
        publish_arrow_version(handle.as_ref(), handle1_version, &[1], &[(1, 1)]);

        let params = MvChangelogParams {
            mv_name: "mv_subscribe_empty_version".to_string(),
            with_snapshot: false,
            as_of: Some(handle1_version),
        };
        let cancel = CancellationToken::new();
        let mut stream = execute_mv_changelog(registry.as_ref(), params, cancel.clone()).await?;

        publish_arrow_version(handle.as_ref(), 2, &[1], &[]);

        let batch = timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("timeout waiting for empty changelog batch")
            .expect("expected empty changelog batch")?;
        assert_eq!(batch.version, 2);
        assert_eq!(batch.batch.num_rows(), 0);
        assert!(batch.diffs.is_empty());

        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn deltas_match_materialized_state() -> MvChangelogResult<()> {
        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_subscribe_delta_validate", build_schema());
        let handle = registry.register("mv_subscribe_delta_validate");

        let handle1_version = 1_i64;
        publish_arrow_version(handle.as_ref(), handle1_version, &[1, 2], &[(1, 1), (2, 1)]);

        let params = MvChangelogParams {
            mv_name: "mv_subscribe_delta_validate".to_string(),
            with_snapshot: true,
            as_of: Some(handle1_version),
        };
        let cancel = CancellationToken::new();
        let mut stream = execute_mv_changelog(registry.as_ref(), params, cancel.clone()).await?;

        let handle2_version = 2_i64;
        publish_arrow_version(
            handle.as_ref(),
            handle2_version,
            &[2, 3],
            &[(1, -1), (3, 1)],
        );

        let handle3_version = 3_i64;
        publish_arrow_version(
            handle.as_ref(),
            handle3_version,
            &[3, 4],
            &[(2, -1), (4, 1)],
        );

        let mut state: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
        for _ in 0..3 {
            let batch = timeout(Duration::from_millis(200), stream.next())
                .await
                .expect("timeout waiting for changelog batch")
                .expect("expected changelog batch")?;
            let values = batch
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int column");
            for idx in 0..values.len() {
                let value = values.value(idx);
                let diff = batch.diffs[idx];
                let entry = state.entry(value).or_insert(0);
                *entry += diff;
                if *entry == 0 {
                    state.remove(&value);
                }
            }
        }

        let mut expected: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
        for value in [3_i64, 4_i64] {
            expected.insert(value, 1);
        }

        assert_eq!(state, expected);
        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn changelog_stream_cancels() -> MvChangelogResult<()> {
        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_subscribe_cancel", build_schema());
        let handle = registry.register("mv_subscribe_cancel");

        publish_arrow_version(handle.as_ref(), 1, &[1], &[(1, 1)]);

        let params = MvChangelogParams {
            mv_name: "mv_subscribe_cancel".to_string(),
            with_snapshot: false,
            as_of: None,
        };
        let cancel = CancellationToken::new();
        let mut stream = execute_mv_changelog(registry.as_ref(), params, cancel.clone()).await?;
        cancel.cancel();
        let err = timeout(Duration::from_millis(100), stream.next())
            .await
            .expect("cancellation timeout")
            .expect("expected cancellation event")
            .expect_err("expected cancellation error");
        assert!(is_mv_changelog_canceled_error(&err));
        Ok(())
    }

    #[tokio::test]
    async fn as_of_with_snapshot_emits_requested_version() -> MvChangelogResult<()> {
        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_subscribe_asof_snap", build_schema());
        let handle = registry.register("mv_subscribe_asof_snap");

        let handle1_version = 1_i64;
        publish_arrow_version(handle.as_ref(), handle1_version, &[10], &[(10, 1)]);
        publish_arrow_version(handle.as_ref(), 2, &[10, 20], &[(20, 1)]);

        let params = MvChangelogParams {
            mv_name: "mv_subscribe_asof_snap".to_string(),
            with_snapshot: true,
            as_of: Some(handle1_version),
        };
        let cancel = CancellationToken::new();
        let mut stream = execute_mv_changelog(registry.as_ref(), params, cancel.clone()).await?;
        let batch = stream.next().await.expect("snapshot batch")?;
        assert_eq!(batch.version, handle1_version);
        assert_eq!(batch.kind, MvChangelogBatchKind::Snapshot);
        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn as_of_with_snapshot_accepts_published_empty_logical_version() -> MvChangelogResult<()>
    {
        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_subscribe_asof_empty_logical", build_schema());
        let handle = registry.register("mv_subscribe_asof_empty_logical");

        publish_arrow_version(handle.as_ref(), 1, &[10], &[(10, 1)]);
        publish_arrow_version(handle.as_ref(), 2, &[10], &[]);

        let params = MvChangelogParams {
            mv_name: "mv_subscribe_asof_empty_logical".to_string(),
            with_snapshot: true,
            as_of: Some(2),
        };
        let cancel = CancellationToken::new();
        let mut stream = execute_mv_changelog(registry.as_ref(), params, cancel.clone()).await?;

        let batch = stream.next().await.expect("snapshot batch")?;
        assert_eq!(batch.version, 2);
        assert_eq!(batch.batch.num_rows(), 1);
        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn as_of_without_snapshot_starts_after_version() -> MvChangelogResult<()> {
        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_subscribe_asof_no_snap", build_schema());
        let handle = registry.register("mv_subscribe_asof_no_snap");

        publish_arrow_version(handle.as_ref(), 1, &[1], &[(1, 1)]);
        let handle2_version = 2_i64;
        publish_arrow_version(handle.as_ref(), handle2_version, &[1, 2], &[(2, 1)]);

        let params = MvChangelogParams {
            mv_name: "mv_subscribe_asof_no_snap".to_string(),
            with_snapshot: false,
            as_of: Some(handle2_version),
        };
        let cancel = CancellationToken::new();
        let mut stream = execute_mv_changelog(registry.as_ref(), params, cancel.clone()).await?;

        assert!(
            timeout(Duration::from_millis(20), stream.next())
                .await
                .is_err()
        );

        let handle3_version = 3_i64;
        publish_arrow_version(handle.as_ref(), handle3_version, &[1, 2, 3], &[(3, 1)]);

        let batch = stream.next().await.expect("post-asof batch")?;
        assert_eq!(batch.version, handle3_version);
        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn with_snapshot_waits_for_first_version_when_view_starts_empty() -> MvChangelogResult<()>
    {
        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_subscribe_empty_start", build_schema());
        let handle = registry.register("mv_subscribe_empty_start");

        let params = MvChangelogParams {
            mv_name: "mv_subscribe_empty_start".to_string(),
            with_snapshot: true,
            as_of: None,
        };
        let cancel = CancellationToken::new();
        let mut stream = execute_mv_changelog(registry.as_ref(), params, cancel.clone()).await?;

        assert!(
            timeout(Duration::from_millis(20), stream.next())
                .await
                .is_err()
        );

        publish_arrow_version(handle.as_ref(), 1, &[11], &[(11, 1)]);

        let batch = timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("timeout waiting for first version")
            .expect("expected first changelog batch")?;
        assert_eq!(batch.version, 1);
        cancel.cancel();
        Ok(())
    }
}
