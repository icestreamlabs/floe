use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::storage::KeyValueTable;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::core::stream::{Stream, StreamEvaluator};
use super::util::{build_derived_stream, build_evaluated_stream};

pub struct StreamAddition<T>
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
    group: Arc<dyn AbelianGroup<T>>,
    table: Arc<dyn KeyValueTable>,
    namespace_prefix: String,
}

impl<T> StreamAddition<T>
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
    pub fn new(
        group: Arc<dyn AbelianGroup<T>>,
        table: Arc<dyn KeyValueTable>,
        namespace_prefix: impl Into<String>,
    ) -> Self {
        Self {
            group,
            table,
            namespace_prefix: namespace_prefix.into(),
        }
    }

    pub fn from_stream(stream: &Stream<T>) -> Self {
        Self::new(stream.group(), stream.table(), "stream_add/")
    }
}

#[async_trait]
impl<T> AbelianGroup<Stream<T>> for StreamAddition<T>
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
    async fn add(&self, a: &Stream<T>, b: &Stream<T>) -> Stream<T> {
        let frontier = a.current_time().max(b.current_time());
        let horizon = a.semantic_horizon().max(b.semantic_horizon());
        build_evaluated_stream(
            self.table.clone(),
            self.group.clone(),
            Arc::new(AddEvaluator {
                left: a.clone(),
                right: b.clone(),
            }),
            &self.namespace_prefix,
            frontier,
            horizon,
        )
        .await
        .expect("failed to construct stream for addition")
    }

    async fn neg(&self, a: &Stream<T>) -> Stream<T> {
        let frontier = a.current_time();
        let horizon = a.semantic_horizon();
        build_evaluated_stream(
            self.table.clone(),
            self.group.clone(),
            Arc::new(NegEvaluator { input: a.clone() }),
            &self.namespace_prefix,
            frontier,
            horizon,
        )
        .await
        .expect("failed to construct stream for negation")
    }

    async fn identity(&self) -> Stream<T> {
        build_derived_stream(
            self.table.clone(),
            self.group.clone(),
            &self.namespace_prefix,
        )
        .await
        .expect("failed to construct stream for identity")
    }
}

struct AddEvaluator<T>
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
    left: Stream<T>,
    right: Stream<T>,
}

#[async_trait]
impl<T> StreamEvaluator<T> for AddEvaluator<T>
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
        let mut left = self.left.clone();
        let mut right = self.right.clone();
        let left_value = left.get(timestamp).await?;
        let right_value = right.get(timestamp).await?;
        Ok(group.add(&left_value, &right_value).await)
    }
}

struct NegEvaluator<T>
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
}

#[async_trait]
impl<T> StreamEvaluator<T> for NegEvaluator<T>
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
        let value = input.get(timestamp).await?;
        Ok(group.neg(&value).await)
    }
}
