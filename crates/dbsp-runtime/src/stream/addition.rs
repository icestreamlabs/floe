use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::storage::KeyValueTable;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::core::stream::{Stream, StreamEvaluator, StreamEvaluatorDescriptor};
use super::util::build_derived_stream;

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
    async fn add(&self, a: &Stream<T>, b: &Stream<T>) -> Result<Stream<T>> {
        let frontier = a.current_time().max(b.current_time());
        let horizon = a.semantic_horizon().max(b.semantic_horizon());
        build_builtin_addition_stream(
            self.table.clone(),
            self.group.clone(),
            Arc::new(AddEvaluator {
                left: a.clone(),
                right: b.clone(),
            }),
            StreamEvaluatorDescriptor::Binary {
                kind: "add",
                left_namespace: a.namespace().to_string(),
                right_namespace: b.namespace().to_string(),
            },
            &self.namespace_prefix,
            frontier,
            horizon,
        )
        .await
    }

    async fn neg(&self, a: &Stream<T>) -> Result<Stream<T>> {
        let frontier = a.current_time();
        let horizon = a.semantic_horizon();
        build_builtin_addition_stream(
            self.table.clone(),
            self.group.clone(),
            Arc::new(NegEvaluator { input: a.clone() }),
            StreamEvaluatorDescriptor::Unary {
                kind: "neg",
                input_namespace: a.namespace().to_string(),
            },
            &self.namespace_prefix,
            frontier,
            horizon,
        )
        .await
    }

    async fn identity(&self) -> Result<Stream<T>> {
        build_derived_stream(
            self.table.clone(),
            self.group.clone(),
            &self.namespace_prefix,
        )
        .await
    }
}

pub(crate) fn builtin_addition_evaluator<T>(
    kind: impl AsRef<str>,
    input: Option<Stream<T>>,
    right: Option<Stream<T>>,
) -> Result<Arc<dyn StreamEvaluator<T>>>
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
    match kind.as_ref() {
        "add" => {
            let Some(left) = input else {
                bail!("add evaluator descriptor missing left input");
            };
            let Some(right) = right else {
                bail!("add evaluator descriptor missing right input");
            };
            Ok(Arc::new(AddEvaluator { left, right }))
        }
        "neg" => {
            let Some(input) = input else {
                bail!("neg evaluator descriptor missing input");
            };
            Ok(Arc::new(NegEvaluator { input }))
        }
        other => bail!("unknown built-in stream evaluator kind `{other}`"),
    }
}

async fn build_builtin_addition_stream<T>(
    table: Arc<dyn KeyValueTable>,
    group: Arc<dyn AbelianGroup<T>>,
    evaluator: Arc<dyn StreamEvaluator<T>>,
    descriptor: StreamEvaluatorDescriptor,
    prefix: &str,
    frontier: i64,
    horizon: i64,
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
    let namespace = super::util::next_derived_namespace(prefix);
    let mut result =
        Stream::evaluated_with_table_and_descriptor(table, namespace, group, evaluator, descriptor)
            .await?;

    for t in 0..=horizon {
        let value = result
            .derived_value_at(t)
            .await?
            .ok_or_else(|| anyhow::anyhow!("built-in addition stream missing evaluator"))?;
        if t == 0 {
            super::util::set_default_in_place(&mut result, value);
        } else if t <= frontier {
            super::util::push_value_in_place(&mut result, value);
        } else {
            super::util::set_value_at_in_place(&result, t, value);
        }
    }

    Ok(result)
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
        group.add(&left_value, &right_value).await
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
        group.neg(&value).await
    }
}
