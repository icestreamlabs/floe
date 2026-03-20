use std::sync::Arc;

use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::storage::KeyValueTable;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::core::stream::Stream;
use super::util::{build_derived_stream, build_exact_stream_from_values, collect_values};

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
        let values_a = collect_values(a, horizon)
            .await
            .expect("collect stream values for left operand");
        let values_b = collect_values(b, horizon)
            .await
            .expect("collect stream values for right operand");

        let mut sums = Vec::with_capacity((horizon + 1) as usize);
        for t in 0..=horizon {
            sums.push(
                self.group
                    .add(&values_a[t as usize], &values_b[t as usize])
                    .await,
            );
        }
        let tail_default = self.group.add(&a.default_value(), &b.default_value()).await;

        build_exact_stream_from_values(
            self.table.clone(),
            self.group.clone(),
            &self.namespace_prefix,
            frontier,
            horizon,
            &sums,
            tail_default,
        )
        .await
        .expect("failed to construct stream for addition")
    }

    async fn neg(&self, a: &Stream<T>) -> Stream<T> {
        let frontier = a.current_time();
        let horizon = a.semantic_horizon();
        let values = collect_values(a, horizon)
            .await
            .expect("collect stream values for negation");
        let mut negated = Vec::with_capacity((horizon + 1) as usize);
        for t in 0..=horizon {
            negated.push(self.group.neg(&values[t as usize]).await);
        }
        let tail_default = self.group.neg(&a.default_value()).await;

        build_exact_stream_from_values(
            self.table.clone(),
            self.group.clone(),
            &self.namespace_prefix,
            frontier,
            horizon,
            &negated,
            tail_default,
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
