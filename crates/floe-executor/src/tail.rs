use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

use anyhow::{Context, Result, anyhow, bail, ensure};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use futures::Stream;
#[cfg(test)]
use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::mv_changelog::{
    MvCatalog, MvChangelogBatch, MvChangelogExecutionConfig, MvChangelogParams, MvChangelogStream,
    execute_mv_changelog_with_config, is_mv_changelog_canceled_error,
};
use floe_sql_parser::{FloeStatement, parse_floe_statement};

/// Alias for the DataFusion session context.
pub type SessionCtx = SessionContext;

pub type PgResult<T> = Result<T>;
const TAIL_STREAM_CHANNEL_CAPACITY_DEFAULT: usize = 256;
const TAIL_MAX_CATCHUP_VERSIONS_DEFAULT: i64 = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TailExecutionConfig {
    pub channel_capacity: usize,
    pub max_catchup_versions: i64,
}

impl Default for TailExecutionConfig {
    fn default() -> Self {
        Self {
            channel_capacity: TAIL_STREAM_CHANNEL_CAPACITY_DEFAULT,
            max_catchup_versions: TAIL_MAX_CATCHUP_VERSIONS_DEFAULT,
        }
    }
}

impl TailExecutionConfig {
    fn channel_capacity(self) -> usize {
        self.channel_capacity.max(1)
    }

    fn max_catchup_versions(self) -> i64 {
        self.max_catchup_versions.max(1)
    }
}

#[derive(Debug)]
pub struct TailBatch {
    pub version: i64,
    pub batch: RecordBatch,
    pub ops: Vec<i16>,
    pub times: Vec<Option<i64>>,
}

pub fn is_tail_canceled_error(err: &anyhow::Error) -> bool {
    is_mv_changelog_canceled_error(err)
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
    inner: MvChangelogStream,
}

impl TailStream {
    fn new(schema: SchemaRef, inner: MvChangelogStream) -> Self {
        Self { schema, inner }
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for TailStream {
    type Item = PgResult<TailBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(tail_batch_from_changelog(batch))),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
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
    execute_tail_with_config(ctx, catalog, params, TailExecutionConfig::default(), cancel).await
}

pub async fn execute_tail_with_config<C>(
    ctx: &SessionCtx,
    catalog: &C,
    params: TailParams,
    config: TailExecutionConfig,
    cancel: CancellationToken,
) -> PgResult<TailStream>
where
    C: MvCatalog + ?Sized,
{
    crate::metrics::init();
    let _ = ctx;
    let output_schema = tail_output_schema(catalog, &params.mv_name)?;
    let stream = execute_mv_changelog_with_config(
        catalog,
        MvChangelogParams {
            mv_name: params.mv_name,
            with_snapshot: params.with_snapshot,
            as_of: params.as_of,
        },
        MvChangelogExecutionConfig {
            channel_capacity: config.channel_capacity(),
            max_catchup_versions: config.max_catchup_versions(),
        },
        cancel,
    )
    .await?;

    Ok(TailStream::new(output_schema, stream))
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

fn tail_batch_from_changelog(batch: MvChangelogBatch) -> PgResult<TailBatch> {
    let ops = batch
        .diffs
        .iter()
        .map(|diff| i16::try_from(*diff).context("MV changelog diff does not fit TAIL __op"))
        .collect::<PgResult<Vec<_>>>()?;
    ensure!(
        ops.len() == batch.batch.num_rows(),
        "TAIL ops length mismatch"
    );
    let times = vec![batch.version_time; batch.batch.num_rows()];
    Ok(TailBatch {
        version: batch.version,
        batch: batch.batch,
        ops,
        times,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dbsp_bridge::DbspBridge;
    use crate::encoding::decode_all_encoded_row_scalars_into;
    use crate::materialized_view::DbspPersistedState;
    use crate::mv::registry::MaterializedViewRegistry;
    use crate::mv::runtime::MaterializedView;
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
    async fn tail_delta_output_is_bounded_by_delta_not_snapshot_size() -> PgResult<()> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("tail-delta-bounded", store).await.expect("db"));
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let mut dbsp_view = bridge
            .new_view(
                "mv_tail_delta_bounded",
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await?;

        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema("mv_tail_delta_bounded", build_schema());
        let handle = registry.register("mv_tail_delta_bounded");

        let initial_values = (0..100).collect::<Vec<_>>();
        let handle1 = append_version(&mut dbsp_view, &initial_values).await?;
        let latest_view = dbsp_view.latest_handle_view();
        let (dict, table, ns, version) = latest_view.into_parts();
        let state = DbspPersistedState::new(dict, table, ns, version);
        handle.set_dbsp_state(state);
        let handle1_version = handle1.version as i64;
        handle.publish_version(handle1_version, handle1);

        let ctx = SessionContext::new();
        let params = TailParams {
            mv_name: "mv_tail_delta_bounded".to_string(),
            with_snapshot: false,
            as_of: Some(handle1_version),
        };
        let cancel = CancellationToken::new();
        let mut stream = execute_tail(&ctx, registry.as_ref(), params, cancel.clone()).await?;

        let handle2 = append_deltas(&mut dbsp_view, &[(1, -1), (101, 1)]).await?;
        let handle2_version = handle2.version as i64;
        handle.publish_version(handle2_version, handle2);

        let batch = timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("timeout waiting for bounded delta batch")
            .expect("expected delta batch")?;
        assert_eq!(batch.version, handle2_version);
        assert_eq!(batch.batch.num_rows(), 2);
        assert_eq!(batch.ops.len(), 2);
        assert!(batch.ops.contains(&-1));
        assert!(batch.ops.contains(&1));

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
        let mut decode_scratch = Vec::new();
        for (key, diff) in snapshot {
            if diff == 0 {
                continue;
            }
            decode_all_encoded_row_scalars_into(&key, &mut decode_scratch)?;
            let value = match decode_scratch.first().and_then(|value| value.as_ref()) {
                Some(crate::encoding::EncodedRowScalar::Int64(v)) => v,
                _ => continue,
            };
            expected.insert(*value, diff);
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
