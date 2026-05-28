use std::hash::Hash;
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
use crate::collections::{DEFAULT_HOT_KEY_COMPACTION_THRESHOLD, IndexedBatchZSet};
use crate::handles::ZSetHandle;
use crate::operators::aggregate::{AggregateOp, AggregateSpec};
use crate::relation_state::RelationState;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::DeltaHandleStream;
use crate::stream::runtime::{
    DeltaOperator, HandleOperatorRuntime, RuntimeErrorHandler, report_runtime_error,
};
use crate::stream::util::{
    build_exact_stream_from_values, collect_values, publish_scheduled_value, push_value_in_place,
};

type BatchKeyExtractor<V, K> = Arc<dyn Fn(&[(V, i64)]) -> Vec<(K, V, i64)> + Send + Sync>;

/// Aggregate wrapper that drives AggregateOp over handle streams.
pub struct DbspAggregate {
    stream: DeltaHandleStream,
}

impl DbspAggregate {
    pub async fn new_batch<K, V, A, FKey>(
        input: &DeltaHandleStream,
        key_extractor: FKey,
        spec: AggregateSpec<K, V, A>,
        error_handler: Option<RuntimeErrorHandler>,
    ) -> anyhow::Result<Self>
    where
        K: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        V: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        A: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        A::Archived: RkyvDeserialize<A, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        FKey: Fn(&[(V, i64)]) -> Vec<(K, V, i64)> + Send + Sync + 'static,
    {
        let table = input.table();
        let frontier = input.current_time();
        let horizon = input.semantic_horizon();
        let aggregate_id = NEXT_AGGREGATE_ID.fetch_add(1, Ordering::Relaxed);
        let output_ns = format!("aggregate_output_{aggregate_id}");
        let empty_handle = ZSetHandle {
            ns: output_ns.clone(),
            version: 0,
        };

        let state =
            RelationState::empty(table.clone(), format!("aggregate_state_{aggregate_id}")).await?;
        let output_dict = Arc::new(
            Dictionary::<(K, A)>::with_table(table.clone(), output_ns.clone(), None)
                .await
                .context("create output dictionary for aggregate")?,
        );
        let output = VersionedZSet::new(output_dict, table.clone(), output_ns.clone())
            .await
            .context("create output zset for aggregate")?;
        let index = IndexedBatchZSet::with_hot_key_compaction_threshold(
            table.clone(),
            format!("aggregate_index_{aggregate_id}"),
            DEFAULT_HOT_KEY_COMPACTION_THRESHOLD,
        );

        let aggregate_op = Arc::new(AsyncMutex::new(AggregateOp::new_batch(
            state,
            index,
            table.clone(),
            Arc::new(key_extractor) as BatchKeyExtractor<V, K>,
            spec,
            output,
        )));

        let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(ZSetHandleGroup {
            default: empty_handle.clone(),
        });

        let history = collect_values(input, horizon).await?;
        let mut output_handles = Vec::with_capacity(history.len());
        for (ts, handle) in history.into_iter().enumerate() {
            let out_handle = {
                let mut op_guard = aggregate_op.lock().await;
                op_guard
                    .on_step(ts as i64, std::slice::from_ref(&handle))
                    .await?
            }
            .unwrap_or_else(|| empty_handle.clone());
            output_handles.push(out_handle);
        }

        let mut stream = build_exact_stream_from_values(
            table.clone(),
            handle_group,
            "aggregate_output_stream/",
            frontier,
            horizon,
            &output_handles,
            empty_handle.clone(),
        )
        .await?;
        stream.flush().await?;
        let writer = Arc::new(AsyncMutex::new(stream.clone()));

        let mut runtime = HandleOperatorRuntime::new(vec![input.stream()], move |ts, handles| {
            let op = Arc::clone(&aggregate_op);
            let writer = Arc::clone(&writer);
            let empty_handle = empty_handle.clone();
            let handles_vec = handles.to_vec();
            Box::pin(async move {
                if handles_vec.len() != 1 {
                    return Err(anyhow::anyhow!(
                        "aggregate runtime expected 1 handle, got {}",
                        handles_vec.len()
                    ));
                }
                if ts <= horizon {
                    let mut writer_guard = writer.lock().await;
                    publish_scheduled_value(&mut writer_guard, ts).await?;
                    return Ok(());
                }
                let mut op_guard = op.lock().await;
                let out_handle = op_guard
                    .on_step(ts, &handles_vec)
                    .await?
                    .unwrap_or_else(|| empty_handle.clone());
                let mut writer_guard = writer.lock().await;
                push_value_in_place(&mut writer_guard, out_handle);
                writer_guard.flush().await?;
                Ok(())
            })
        });

        let error_handler = error_handler.clone();
        tokio::spawn(async move {
            loop {
                if let Err(err) = runtime.step().await {
                    report_runtime_error(&error_handler, "aggregate", err);
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

static NEXT_AGGREGATE_ID: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StreamRetention;
    use crate::operators::aggregate::count_all;
    use crate::storage::SlateTable;
    use crate::stream::util::materialize_zset_handle;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::collections::HashMap;

    #[tokio::test]
    async fn aggregate_wrapper_counts_by_extracted_key() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(
            Db::open("aggregate-wrapper-test", store)
                .await
                .expect("open db"),
        );
        let table: Arc<dyn crate::storage::KeyValueTable> = Arc::new(SlateTable::new(db));

        let dict = Arc::new(
            Dictionary::with_table(table.clone(), "aggregate_wrapper_source", None)
                .await
                .expect("source dictionary"),
        );
        let mut source = crate::ZSetStream::new(
            dict,
            table.clone(),
            "aggregate_wrapper_source",
            StreamRetention::None,
        )
        .await
        .expect("source stream");

        source.add_delta("a".to_string(), 1);
        source.flush().await.expect("flush t1");
        source.add_delta("a".to_string(), 1);
        source.add_delta("cc".to_string(), 1);
        source.flush().await.expect("flush t2");

        let aggregate = DbspAggregate::new_batch::<String, String, i64, _>(
            &source.delta_handle_stream(),
            |delta_values: &[(String, i64)]| {
                delta_values
                    .iter()
                    .map(|(value, weight)| (value.clone(), value.clone(), *weight))
                    .collect()
            },
            count_all::<String, String>(),
            None,
        )
        .await
        .expect("build aggregate wrapper");

        let mut output = aggregate.stream();
        let ts = output.current_time();
        let handle = output.get(ts).await.expect("output handle");
        let mut cache = HashMap::new();
        let materialized =
            materialize_zset_handle::<(String, i64)>(table.clone(), &mut cache, &handle)
                .await
                .expect("materialize aggregate output");

        assert_eq!(materialized.get(&("a".to_string(), 2)), Some(&1));
        assert_eq!(materialized.get(&("cc".to_string(), 1)), Some(&1));
    }
}
