use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

use anyhow::{Context, Result, anyhow, bail};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::scalar::ScalarValue;
use futures::Stream;
#[cfg(test)]
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::encoding::decode_projected_row_key;
use crate::materialized_view::{MaterializedViewHandle, MaterializedViewRegistry};
use crate::mv::runtime::MaterializedView;
use crate::stream_types::Row;
use floe_sql_parser::{FloeStatement, parse_floe_statement};

/// Alias for the DataFusion session context.
pub type SessionCtx = SessionContext;

pub type PgResult<T> = Result<T>;

#[derive(Debug)]
pub struct TailBatch {
    pub version: i64,
    pub batch: RecordBatch,
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

    let (tx, rx) = mpsc::channel(8);
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

    if let Some(as_of_version) = as_of {
        let _ = mv.handle_for(as_of_version)?;
        if with_snapshot {
            emit_version(mv.as_ref(), &schema, as_of_version, tx).await?;
        }
        last_emitted = as_of_version;
    } else if with_snapshot {
        let version =
            latest.ok_or_else(|| anyhow!("materialized view '{}' has no versions yet", mv_name))?;
        emit_version(mv.as_ref(), &schema, version, tx).await?;
        last_emitted = version;
    } else {
        last_emitted = latest.unwrap_or(-1);
    }

    loop {
        let latest_now = mv.latest_version().unwrap_or(last_emitted);
        if latest_now > last_emitted {
            for version in last_emitted + 1..=latest_now {
                emit_version(mv.as_ref(), &schema, version, tx).await?;
                last_emitted = version;
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

async fn emit_version<M: MaterializedView>(
    mv: &M,
    schema: &SchemaRef,
    version: i64,
    tx: &mut mpsc::Sender<PgResult<TailBatch>>,
) -> PgResult<()> {
    let batches = materialize_version_batches(mv, Arc::clone(schema), version).await?;
    for batch in batches {
        let payload = TailBatch { version, batch };
        if tx.send(Ok(payload)).await.is_err() {
            break;
        }
    }
    Ok(())
}

async fn materialize_version_batches<M: MaterializedView>(
    mv: &M,
    schema: SchemaRef,
    version: i64,
) -> PgResult<Vec<RecordBatch>> {
    let handle = mv.handle_for(version)?;
    let snapshot = handle.materialize().await?;
    let rows = rows_from_snapshot(snapshot)?;
    let batches = build_record_batches(rows, schema)?;
    Ok(batches)
}

fn rows_from_snapshot(snapshot: HashMap<Vec<u8>, i64>) -> PgResult<Vec<Row>> {
    let mut rows = Vec::new();
    for (key, diff) in snapshot {
        if diff < 0 {
            bail!("materialized view snapshot contains negative diff {diff}");
        }
        if diff == 0 {
            continue;
        }
        let decoded = decode_projected_row_key(&key)?;
        for _ in 0..diff {
            rows.push(decoded.clone());
        }
    }
    Ok(rows)
}

fn build_record_batches(rows: Vec<Row>, schema: SchemaRef) -> PgResult<Vec<RecordBatch>> {
    if rows.is_empty() {
        return Ok(vec![RecordBatch::new_empty(schema)]);
    }
    let column_count = schema.fields().len();
    let mut columns: Vec<Vec<ScalarValue>> = vec![Vec::with_capacity(rows.len()); column_count];
    for row in rows {
        if row.len() != column_count {
            bail!(
                "row has {} columns but schema has {}",
                row.len(),
                column_count
            );
        }
        for (idx, value) in row.into_iter().enumerate() {
            columns[idx].push(value);
        }
    }
    let arrays: Vec<ArrayRef> = columns
        .into_iter()
        .enumerate()
        .map(|(idx, col)| {
            ScalarValue::iter_to_array(col)
                .with_context(|| format!("convert tail column {idx} to array"))
        })
        .collect::<PgResult<_>>()?;
    let batch = RecordBatch::try_new(schema, arrays)?;
    Ok(vec![batch])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dbsp_bridge::DbspBridge;
    use crate::encoding::encode_projected_row_key;
    use crate::materialized_view::DbspPersistedState;
    use crate::mv::registry::MaterializedViewRegistry;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
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

    fn scalar_row(value: i64) -> Row {
        vec![ScalarValue::Int64(Some(value))]
    }

    async fn append_version(
        view: &mut crate::dbsp_bridge::DbspView,
        values: &[i64],
    ) -> PgResult<dbsp::handles::ZSetHandle> {
        for value in values {
            let row = scalar_row(*value);
            let encoded = encode_projected_row_key(&row)?;
            view.add_delta(encoded, 1);
        }
        view.flush().await
    }

    #[tokio::test]
    async fn snapshot_then_streams_new_versions() -> PgResult<()> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("tail-test", store).await.expect("db"));
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let mut dbsp_view = bridge.new_view("mv_tail").await?;

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
    async fn tail_stream_cancels() -> PgResult<()> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("tail-cancel", store).await.expect("db"));
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let mut dbsp_view = bridge.new_view("mv_tail_cancel").await?;

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
        let err = stream
            .next()
            .await
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
        let mut dbsp_view = bridge.new_view("mv_tail_asof_snap").await?;

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
    async fn as_of_without_snapshot_starts_after_version() -> PgResult<()> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("tail-asof-no-snap", store).await.expect("db"));
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let mut dbsp_view = bridge.new_view("mv_tail_asof_no_snap").await?;

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
}
