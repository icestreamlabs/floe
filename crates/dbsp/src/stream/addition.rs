use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::storage::KeyValueTable;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::core::stream::Stream;
use super::util::{collect_values, push_value_in_place, set_default_in_place};

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
    counter: AtomicU64,
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
            counter: AtomicU64::new(0),
        }
    }

    pub fn from_stream(stream: &Stream<T>) -> Self {
        Self::new(stream.group(), stream.table(), "stream_add/")
    }

    fn next_namespace(&self) -> String {
        let id = self.counter.fetch_add(1, Ordering::Relaxed);
        format!("{}{}", self.namespace_prefix, id)
    }

    async fn build_stream(&self) -> Result<Stream<T>> {
        let namespace = self.next_namespace();
        Stream::with_table(self.table.clone(), namespace, self.group.clone()).await
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
        let max_ts = a.current_time().max(b.current_time());
        let values_a = collect_values(a, max_ts)
            .await
            .expect("collect stream values for left operand");
        let values_b = collect_values(b, max_ts)
            .await
            .expect("collect stream values for right operand");

        let mut result = self
            .build_stream()
            .await
            .expect("failed to construct stream for addition");
        if !values_a.is_empty() && !values_b.is_empty() {
            let default_value = self.group.add(&values_a[0], &values_b[0]).await;
            set_default_in_place(&mut result, default_value.clone());

            for t in 1..=max_ts {
                let sum = self
                    .group
                    .add(&values_a[t as usize], &values_b[t as usize])
                    .await;
                push_value_in_place(&mut result, sum);
            }
        }

        result
    }

    async fn neg(&self, a: &Stream<T>) -> Stream<T> {
        let max_ts = a.current_time();
        let values = collect_values(a, max_ts)
            .await
            .expect("collect stream values for negation");
        let mut result = self
            .build_stream()
            .await
            .expect("failed to construct stream for negation");

        if let Some(first) = values.first() {
            let default_value = self.group.neg(first).await;
            set_default_in_place(&mut result, default_value.clone());

            for t in 1..=max_ts {
                let value = self.group.neg(&values[t as usize]).await;
                push_value_in_place(&mut result, value);
            }
        }

        result
    }

    async fn identity(&self) -> Stream<T> {
        self.build_stream()
            .await
            .expect("failed to construct stream for identity")
    }
}
