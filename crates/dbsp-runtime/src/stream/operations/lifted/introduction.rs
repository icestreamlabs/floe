use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::handles::StreamHandle;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::core::stream::{Stream, StreamEvaluator};
use crate::stream::groups::HandleGroup;
use crate::stream::util::build_evaluated_stream;

use super::super::basic::stream_introduction;

pub async fn lifted_stream_introduction<T>(stream: &Stream<T>) -> Result<Stream<StreamHandle>>
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
    let group = stream.group();
    let table = stream.table();

    let mut default_stream =
        stream_introduction(table.clone(), group.clone(), stream.default_value()).await?;
    default_stream.flush().await?;
    let default_handle = default_stream.handle();

    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    build_evaluated_stream(
        table,
        handle_group,
        Arc::new(LiftedIntroductionEvaluator {
            input: stream.clone(),
            inner_group: group,
        }),
        "stream_lift_intro/",
        frontier,
        horizon,
    )
    .await
}

struct LiftedIntroductionEvaluator<T>
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
    input: Stream<T>,
    inner_group: Arc<dyn AbelianGroup<T>>,
}

#[async_trait]
impl<T> StreamEvaluator<StreamHandle> for LiftedIntroductionEvaluator<T>
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
    async fn value_at(
        &self,
        timestamp: i64,
        _group: Arc<dyn AbelianGroup<StreamHandle>>,
    ) -> Result<StreamHandle> {
        let mut input = self.input.clone();
        let value = input.get(timestamp).await?;
        let mut introduced =
            stream_introduction(input.table(), self.inner_group.clone(), value).await?;
        introduced.flush().await?;
        Ok(introduced.handle())
    }
}
