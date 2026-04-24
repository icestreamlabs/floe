use anyhow::Result;
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

use crate::algebra::AbelianGroup;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::core::stream::{Stream, StreamEvaluator};
use crate::stream::util::build_evaluated_stream;

/// Exact one-tick delay over the represented total stream.
///
/// The result is evaluated as `z^-1(input)` for every logical timestamp; the
/// materialized prefix below is only a cache of already-observed values.
pub async fn delay<T>(input: &Stream<T>) -> Result<Stream<T>>
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
    let frontier = input.current_time();
    let horizon = input.semantic_horizon() + 1;
    build_evaluated_stream(
        input.table(),
        input.group(),
        Arc::new(DelayEvaluator {
            input: input.clone(),
        }),
        "stream_delay/",
        frontier,
        horizon,
    )
    .await
}

/// Exact stream differentiation through the input semantic horizon.
///
/// The resulting stream is eventually the group identity after the final
/// scheduled transition.
pub async fn differentiate<T>(input: &Stream<T>) -> Result<Stream<T>>
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
    let frontier = input.current_time();
    let horizon = input.semantic_horizon() + 1;
    build_evaluated_stream(
        input.table(),
        input.group(),
        Arc::new(DifferentiateEvaluator {
            input: input.clone(),
        }),
        "stream_diff/",
        frontier,
        horizon,
    )
    .await
}

/// Stateful prefix integration.
///
/// The derived stream computes additional timestamps on demand, so non-identity
/// input tails remain exact as logical time advances.
pub async fn integrate<T>(input: &Stream<T>) -> Result<Stream<T>>
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
    let frontier = input.current_time();
    let horizon = input.semantic_horizon();
    build_evaluated_stream(
        input.table(),
        input.group(),
        Arc::new(IntegrateEvaluator {
            input: input.clone(),
            cache: Mutex::new(BTreeMap::new()),
        }),
        "stream_integrate/",
        frontier,
        horizon,
    )
    .await
}

struct DelayEvaluator<T>
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
impl<T> StreamEvaluator<T> for DelayEvaluator<T>
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
        if timestamp == 0 {
            Ok(group.identity().await)
        } else {
            let mut input = self.input.clone();
            input.get(timestamp - 1).await
        }
    }
}

struct DifferentiateEvaluator<T>
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
impl<T> StreamEvaluator<T> for DifferentiateEvaluator<T>
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
        let current = input.get(timestamp).await?;
        if timestamp == 0 {
            Ok(current)
        } else {
            let previous = input.get(timestamp - 1).await?;
            let neg_previous = group.neg(&previous).await;
            Ok(group.add(&current, &neg_previous).await)
        }
    }
}

struct IntegrateEvaluator<T>
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
    cache: Mutex<BTreeMap<i64, T>>,
}

#[async_trait]
impl<T> StreamEvaluator<T> for IntegrateEvaluator<T>
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
        let cached = {
            let cache = self
                .cache
                .lock()
                .expect("integrate evaluator cache lock poisoned");
            cache
                .range(..=timestamp)
                .next_back()
                .map(|(&cached_ts, cached_value)| (cached_ts, cached_value.clone()))
        };

        let (start, mut acc) = if let Some((cached_ts, cached_value)) = cached {
            (cached_ts + 1, cached_value)
        } else {
            (0, group.identity().await)
        };

        for t in start..=timestamp {
            let value = input.get(t).await?;
            acc = group.add(&acc, &value).await;
            self.cache
                .lock()
                .expect("integrate evaluator cache lock poisoned")
                .insert(t, acc.clone());
        }
        Ok(acc)
    }
}
