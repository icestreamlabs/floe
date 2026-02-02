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
    build_derived_stream, collect_values, push_value_in_place, set_default_in_place,
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

        let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(ZSetHandleGroup {
            default: ZSetHandle {
                ns: output_ns.clone(),
                version: 0,
            },
        });
        let mut stream =
            build_derived_stream(table.clone(), handle_group, "union_output_stream/").await?;
        set_default_in_place(
            &mut stream,
            ZSetHandle {
                ns: output_ns,
                version: 0,
            },
        );

        let writer = Arc::new(AsyncMutex::new(stream.clone()));

        let mut histories = Vec::with_capacity(inputs.len());
        let mut defaults = Vec::with_capacity(inputs.len());
        let mut max_len = 0usize;
        for input in inputs {
            let history = collect_values(input, input.current_time()).await?;
            max_len = max_len.max(history.len());
            histories.push(history);
            defaults.push(input.default_value());
        }

        for ts in 0..max_len {
            let mut handles = Vec::with_capacity(inputs.len());
            for (idx, history) in histories.iter().enumerate() {
                let handle = history
                    .get(ts)
                    .cloned()
                    .unwrap_or_else(|| defaults[idx].clone());
                handles.push(handle);
            }
            drive_union(&union_op, &writer, ts as i64, handles).await?;
        }

        let streams: Vec<Stream<ZSetHandle>> = inputs.iter().map(|input| input.stream()).collect();
        let op = Arc::clone(&union_op);
        let mut runtime = HandleOperatorRuntime::new(streams, move |ts, handles| {
            let op = Arc::clone(&op);
            let writer = Arc::clone(&writer);
            let handles = handles.to_vec();
            Box::pin(async move { drive_union(&op, &writer, ts, handles).await })
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

        stream.flush().await?;
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
    if let Some(out) = op_guard.on_step(ts, &handles).await? {
        let mut writer_guard = writer.lock().await;
        push_value_in_place(&mut writer_guard, out);
        writer_guard.flush().await?;
    }
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
