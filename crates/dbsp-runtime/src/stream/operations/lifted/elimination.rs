use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::handles::StreamHandle;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::core::stream::{Stream, StreamEvaluator};
use crate::stream::util::build_evaluated_stream;

pub async fn lifted_stream_elimination<T>(
    stream: &Stream<StreamHandle>,
    inner_group: Arc<dyn AbelianGroup<T>>,
) -> Result<Stream<T>>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let frontier = stream.current_time();
    let horizon = stream.semantic_horizon();

    build_evaluated_stream(
        stream.table(),
        inner_group,
        Arc::new(LiftedEliminationEvaluator {
            input: stream.clone(),
        }),
        "stream_lift_elim/",
        frontier,
        horizon,
    )
    .await
}

struct LiftedEliminationEvaluator {
    input: Stream<StreamHandle>,
}

#[async_trait]
impl<T> StreamEvaluator<T> for LiftedEliminationEvaluator
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    async fn value_at(&self, timestamp: i64, group: Arc<dyn AbelianGroup<T>>) -> Result<T> {
        let mut input = self.input.clone();
        let handle = input.get(timestamp).await?;
        let mut inner = self
            .input
            .resolve_handle(&handle, group)
            .await
            .context("resolve handle for lifted stream elimination evaluator")?;
        inner
            .latest()
            .await
            .context("load latest handle for lifted stream elimination evaluator")
    }
}
