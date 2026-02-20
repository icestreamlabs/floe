use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Context;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use tokio::sync::Mutex as AsyncMutex;

use crate::algebra::AbelianGroup;
use crate::collections::zset::VersionedZSet;
use crate::handles::ZSetHandle;
use crate::operators::topn::TopNOp;
use crate::relation_state::RelationState;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::DeltaHandleStream;
use crate::stream::runtime::{
    DeltaOperator, HandleOperatorRuntime, RuntimeErrorHandler, report_runtime_error,
};
use crate::stream::util::{
    build_derived_stream, collect_values, push_value_in_place, set_default_in_place,
};

/// TopN wrapper that drives TopNOp over handle streams (row-number semantics).
pub struct DbspTopN {
    stream: DeltaHandleStream,
}

impl DbspTopN {
    pub async fn new<K, P, O, FP, FO>(
        input: &DeltaHandleStream,
        partition_key: FP,
        order_key: FO,
        limit: usize,
        offset: usize,
        error_handler: Option<RuntimeErrorHandler>,
    ) -> anyhow::Result<Self>
    where
        K: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Ord
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        P: Ord + Clone + Send + Sync + 'static,
        O: Ord + Clone + Send + Sync + 'static,
        FP: Fn(&K) -> Option<P> + Send + Sync + Clone + 'static,
        FO: Fn(&K) -> Option<O> + Send + Sync + Clone + 'static,
    {
        let table = input.table();
        let topn_id = NEXT_TOPN_ID.fetch_add(1, Ordering::Relaxed);
        let output_ns = format!("topn_output_{topn_id}");

        let state = RelationState::empty(table.clone(), format!("topn_state_{topn_id}")).await?;
        let output_dict = Arc::new(
            Dictionary::<K>::with_table(table.clone(), output_ns.clone(), None)
                .await
                .context("create output dictionary for topn")?,
        );
        let output = VersionedZSet::new(output_dict, table.clone(), output_ns.clone())
            .await
            .context("create output zset for topn")?;

        let topn_op = Arc::new(AsyncMutex::new(TopNOp::new(
            state,
            table.clone(),
            output,
            Arc::new(partition_key),
            Arc::new(order_key),
            limit,
            offset,
        )));

        let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(ZSetHandleGroup {
            default: ZSetHandle {
                ns: output_ns.clone(),
                version: 0,
            },
        });
        let mut stream =
            build_derived_stream(table.clone(), handle_group, "topn_output_stream/").await?;
        set_default_in_place(
            &mut stream,
            ZSetHandle {
                ns: output_ns,
                version: 0,
            },
        );

        let writer = Arc::new(AsyncMutex::new(stream.clone()));

        let history = collect_values(input, input.current_time()).await?;
        for (ts, handle) in history.into_iter().enumerate() {
            let out_handle = {
                let mut op_guard = topn_op.lock().await;
                op_guard
                    .on_step(ts as i64, std::slice::from_ref(&handle))
                    .await?
            };
            if let Some(out_handle) = out_handle {
                let mut writer_guard = writer.lock().await;
                push_value_in_place(&mut writer_guard, out_handle);
                writer_guard.flush().await?;
            }
        }

        let mut runtime = HandleOperatorRuntime::new(vec![input.stream()], move |ts, handles| {
            let op = Arc::clone(&topn_op);
            let writer = Arc::clone(&writer);
            let handles_vec = handles.to_vec();
            Box::pin(async move {
                if handles_vec.len() != 1 {
                    return Err(anyhow::anyhow!(
                        "topn runtime expected 1 handle, got {}",
                        handles_vec.len()
                    ));
                }
                let mut op_guard = op.lock().await;
                if let Some(out_handle) = op_guard.on_step(ts, &handles_vec).await? {
                    let mut writer_guard = writer.lock().await;
                    push_value_in_place(&mut writer_guard, out_handle);
                    writer_guard.flush().await?;
                }
                Ok(())
            })
        });

        let error_handler = error_handler.clone();
        tokio::spawn(async move {
            loop {
                if let Err(err) = runtime.step().await {
                    report_runtime_error(&error_handler, "topn", err);
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

static NEXT_TOPN_ID: AtomicUsize = AtomicUsize::new(0);
