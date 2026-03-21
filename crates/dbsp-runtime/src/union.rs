use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use tokio::sync::Mutex as AsyncMutex;

use crate::algebra::AbelianGroup;
use crate::collections::zset::VersionedZSet;
use crate::handles::ZSetHandle;
use crate::operators::union::UnionOp;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::DeltaHandleStream;
use crate::stream::Stream;
use crate::stream::runtime::{
    DeltaOperator, HandleOperatorRuntime, RuntimeErrorHandler, report_runtime_error,
};
use crate::stream::util::{
    build_exact_stream_from_values, collect_values, publish_scheduled_value, push_value_in_place,
};

/// Union wrapper that drives UnionOp over multiple handle streams.
pub struct DbspUnion {
    stream: DeltaHandleStream,
}

impl DbspUnion {
    pub async fn new<K>(
        inputs: &[DeltaHandleStream],
        error_handler: Option<RuntimeErrorHandler>,
    ) -> Result<Self>
    where
        K: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    {
        if inputs.is_empty() {
            return Err(anyhow!("union requires at least one input"));
        }

        let table = inputs[0].table();
        let frontier = inputs
            .iter()
            .map(|input| input.current_time())
            .max()
            .unwrap_or(0);
        let horizon = inputs
            .iter()
            .map(|input| input.semantic_horizon())
            .max()
            .unwrap_or(0);
        for input in inputs.iter().skip(1) {
            if !Arc::ptr_eq(&table, &input.table()) {
                return Err(anyhow!("union inputs must share the same backing table"));
            }
        }

        let union_id = NEXT_UNION_ID.fetch_add(1, Ordering::Relaxed);
        let output_ns = format!("union_output_{union_id}");
        let output_dict = Arc::new(
            Dictionary::<K>::with_table(table.clone(), output_ns.clone(), None)
                .await
                .context("create output dictionary for union")?,
        );
        let output = VersionedZSet::new(output_dict, table.clone(), output_ns.clone())
            .await
            .context("create output zset for union")?;

        let union_op = Arc::new(AsyncMutex::new(UnionOp::new(table.clone(), output)));
        let empty_handle = ZSetHandle {
            ns: output_ns.clone(),
            version: 0,
        };

        let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(ZSetHandleGroup {
            default: empty_handle.clone(),
        });

        let mut histories = Vec::with_capacity(inputs.len());
        for input in inputs {
            let history = collect_values(input, horizon).await?;
            histories.push(history);
        }

        let mut output_handles = Vec::with_capacity((horizon + 1) as usize);
        for ts in 0..=horizon {
            let mut handles = Vec::with_capacity(inputs.len());
            for history in &histories {
                handles.push(history[ts as usize].clone());
            }
            let out_handle = {
                let mut op_guard = union_op.lock().await;
                op_guard.on_step(ts, &handles).await?
            }
            .unwrap_or_else(|| empty_handle.clone());
            output_handles.push(out_handle);
        }

        let mut stream = build_exact_stream_from_values(
            table.clone(),
            handle_group,
            "union_output_stream/",
            frontier,
            horizon,
            &output_handles,
            empty_handle.clone(),
        )
        .await?;
        stream.flush().await?;

        let writer = Arc::new(AsyncMutex::new(stream.clone()));

        let streams: Vec<Stream<ZSetHandle>> = inputs.iter().map(|input| input.stream()).collect();
        let op = Arc::clone(&union_op);
        let mut runtime = HandleOperatorRuntime::new(streams, move |ts, handles| {
            let op = Arc::clone(&op);
            let writer = Arc::clone(&writer);
            let empty_handle = empty_handle.clone();
            let handles = handles.to_vec();
            Box::pin(async move {
                if ts <= horizon {
                    let mut writer_guard = writer.lock().await;
                    publish_scheduled_value(&mut writer_guard, ts).await?;
                    return Ok(());
                }
                drive_union(&op, &writer, &empty_handle, ts, handles).await
            })
        });

        let error_handler = error_handler.clone();
        tokio::spawn(async move {
            loop {
                if let Err(err) = runtime.step().await {
                    report_runtime_error(&error_handler, "union", err);
                    break;
                }
            }
        });

        Ok(Self {
            stream: DeltaHandleStream::new(stream),
        })
    }

    pub fn stream(&self) -> DeltaHandleStream {
        self.stream.clone()
    }
}

async fn drive_union<K>(
    op: &Arc<AsyncMutex<UnionOp<K>>>,
    writer: &Arc<AsyncMutex<Stream<ZSetHandle>>>,
    empty_handle: &ZSetHandle,
    ts: i64,
    handles: Vec<ZSetHandle>,
) -> Result<()>
where
    K: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let mut op_guard = op.lock().await;
    let out = op_guard
        .on_step(ts, &handles)
        .await?
        .unwrap_or_else(|| empty_handle.clone());
    let mut writer_guard = writer.lock().await;
    push_value_in_place(&mut writer_guard, out);
    writer_guard.flush().await?;
    Ok(())
}

#[derive(Clone)]
struct ZSetHandleGroup {
    default: ZSetHandle,
}

#[async_trait::async_trait]
impl AbelianGroup<ZSetHandle> for ZSetHandleGroup {
    async fn add(&self, a: &ZSetHandle, _b: &ZSetHandle) -> ZSetHandle {
        a.clone()
    }

    async fn neg(&self, a: &ZSetHandle) -> ZSetHandle {
        a.clone()
    }

    async fn identity(&self) -> ZSetHandle {
        self.default.clone()
    }
}

static NEXT_UNION_ID: AtomicUsize = AtomicUsize::new(0);
