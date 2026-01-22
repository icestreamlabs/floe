use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::collections::IndexedZSet;
use crate::handles::ZSetHandle;
use crate::operators::group_by::GroupByOp;
use crate::relation_state::RelationState;
use crate::storage::KeyValueTable;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::DeltaOperator;

type KeyExtractor<V, K> = Arc<dyn Fn(&V) -> Option<K> + Send + Sync>;
type Aggregator<K, V, A> = Arc<dyn Fn(&K, &[(V, i64)]) -> Option<A> + Send + Sync>;

/// Aggregate specification used by `AggregateOp`.
///
/// This keeps the aggregation logic explicit while we mirror the Feldera
/// approach of recomputing per-key aggregates from an arranged index.
pub struct AggregateSpec<K, V, A>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Eq + Hash + Send + Sync + 'static,
    A: Clone + Eq + Hash + Send + Sync + 'static,
{
    name: String,
    aggregator: Aggregator<K, V, A>,
}

impl<K, V, A> AggregateSpec<K, V, A>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Eq + Hash + Send + Sync + 'static,
    A: Clone + Eq + Hash + Send + Sync + 'static,
{
    pub fn new(
        name: impl Into<String>,
        aggregator: impl Fn(&K, &[(V, i64)]) -> Option<A> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            aggregator: Arc::new(aggregator),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

pub fn count_all<K, V>() -> AggregateSpec<K, V, i64>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Eq + Hash + Send + Sync + 'static,
{
    count_by("count", |_value| true)
}

pub fn count_by<K, V>(
    name: impl Into<String>,
    include: impl Fn(&V) -> bool + Send + Sync + 'static,
) -> AggregateSpec<K, V, i64>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Eq + Hash + Send + Sync + 'static,
{
    AggregateSpec::new(name, move |_key, values| {
        let mut count = 0i64;
        let mut has_rows = false;
        for (value, weight) in values {
            if *weight == 0 {
                continue;
            }
            has_rows = true;
            if include(value) {
                count += *weight;
            }
        }
        if has_rows {
            Some(count)
        } else {
            None
        }
    })
}

pub fn sum_i64<K, V>(
    name: impl Into<String>,
    extractor: impl Fn(&V) -> Option<i64> + Send + Sync + 'static,
) -> AggregateSpec<K, V, i64>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Eq + Hash + Send + Sync + 'static,
{
    AggregateSpec::new(name, move |_key, values| {
        let mut sum = 0i64;
        let mut has_value = false;
        for (value, weight) in values {
            if *weight == 0 {
                continue;
            }
            if let Some(number) = extractor(value) {
                sum += number * *weight;
                has_value = true;
            }
        }
        if has_value {
            Some(sum)
        } else {
            None
        }
    })
}

pub fn avg_i64<K, V>(
    name: impl Into<String>,
    extractor: impl Fn(&V) -> Option<i64> + Send + Sync + 'static,
) -> AggregateSpec<K, V, i64>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Eq + Hash + Send + Sync + 'static,
{
    AggregateSpec::new(name, move |_key, values| {
        let mut sum = 0i64;
        let mut count = 0i64;
        for (value, weight) in values {
            if *weight == 0 {
                continue;
            }
            if let Some(number) = extractor(value) {
                sum += number * *weight;
                count += *weight;
            }
        }
        if count != 0 {
            Some(sum / count)
        } else {
            None
        }
    })
}

pub fn min_by<K, V, T>(
    name: impl Into<String>,
    extractor: impl Fn(&V) -> Option<T> + Send + Sync + 'static,
) -> AggregateSpec<K, V, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Eq + Hash + Ord + Send + Sync + 'static,
{
    AggregateSpec::new(name, move |_key, values| {
        let mut weights: HashMap<T, i64> = HashMap::new();
        for (value, weight) in values {
            if let Some(mapped) = extractor(value) {
                let entry = weights.entry(mapped.clone()).or_insert(0);
                *entry += *weight;
                if *entry == 0 {
                    weights.remove(&mapped);
                }
            }
        }
        weights.keys().cloned().min()
    })
}

pub fn max_by<K, V, T>(
    name: impl Into<String>,
    extractor: impl Fn(&V) -> Option<T> + Send + Sync + 'static,
) -> AggregateSpec<K, V, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Eq + Hash + Ord + Send + Sync + 'static,
{
    AggregateSpec::new(name, move |_key, values| {
        let mut weights: HashMap<T, i64> = HashMap::new();
        for (value, weight) in values {
            if let Some(mapped) = extractor(value) {
                let entry = weights.entry(mapped.clone()).or_insert(0);
                *entry += *weight;
                if *entry == 0 {
                    weights.remove(&mapped);
                }
            }
        }
        weights.keys().cloned().max()
    })
}

pub fn min_value<K, V>() -> AggregateSpec<K, V, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Eq + Hash + Ord + Send + Sync + 'static,
{
    min_by("min", |value: &V| Some(value.clone()))
}

pub fn max_value<K, V>() -> AggregateSpec<K, V, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Eq + Hash + Ord + Send + Sync + 'static,
{
    max_by("max", |value: &V| Some(value.clone()))
}

pub fn sum_i64_identity<K>() -> AggregateSpec<K, i64, i64>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
{
    sum_i64("sum", |value| Some(*value))
}

pub fn avg_i64_identity<K>() -> AggregateSpec<K, i64, i64>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
{
    avg_i64("avg", |value| Some(*value))
}

pub struct AggregateOp<K, V, A>
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
{
    spec: AggregateSpec<K, V, A>,
    inner: GroupByOp<K, V, A>,
}

impl<K, V, A> AggregateOp<K, V, A>
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
{
    pub fn new(
        state: RelationState<(K, A)>,
        index: IndexedZSet<K, V>,
        table: Arc<dyn KeyValueTable>,
        key_extractor: KeyExtractor<V, K>,
        spec: AggregateSpec<K, V, A>,
        output: crate::collections::zset::VersionedZSet<(K, A)>,
    ) -> Self {
        let aggregator = spec.aggregator.clone();
        let inner = GroupByOp::new(state, index, table, key_extractor, aggregator, output);
        Self { spec, inner }
    }

    pub fn spec(&self) -> &AggregateSpec<K, V, A> {
        &self.spec
    }
}

#[async_trait]
impl<K, V, A> DeltaOperator for AggregateOp<K, V, A>
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
{
    async fn on_step(
        &mut self,
        ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        self.inner
            .on_step(ts, inputs)
            .await
            .with_context(|| format!("aggregate {}", self.spec.name()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::zset::{SegmentRecord, VersionedZSet};
    use crate::storage::dictionary::Dictionary;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::sync::Arc;

    fn bucket_for(id: u64) -> u16 {
        (id >> 48) as u16
    }

    async fn stage_version(
        dict: Arc<Dictionary<i64>>,
        table: Arc<dyn KeyValueTable>,
        namespace: &str,
        deltas: &[(i64, i64)],
    ) -> ZSetHandle {
        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        let mut dict_batch = dict.batch();
        for (key, delta) in deltas {
            let id = dict_batch
                .intern(key)
                .await
                .expect("intern test key for aggregate");
            buckets
                .entry(bucket_for(id))
                .or_default()
                .push((id, *delta));
        }
        drop(dict_batch);

        let mut segments = Vec::new();
        for (bucket, mut bucket_deltas) in buckets {
            bucket_deltas.retain(|(_, delta)| *delta != 0);
            if bucket_deltas.is_empty() {
                continue;
            }
            bucket_deltas.sort_by_key(|(id, _)| *id);
            segments.push(SegmentRecord {
                id: 0,
                bucket,
                deltas: bucket_deltas,
            });
        }

        let mut versioned = VersionedZSet::new(dict, table, namespace.to_string())
            .await
            .expect("build versioned");
        let version = versioned
            .create_version_with_base(segments, None)
            .await
            .expect("create version");
        versioned.handle_for_version(version)
    }

    async fn build_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(Db::open("aggregate", store).await.expect("open SlateDB"))
    }

    fn recompute_expected(
        key_extractor: &KeyExtractor<i64, i64>,
        spec: &AggregateSpec<i64, i64, i64>,
        input_state: &HashMap<i64, i64>,
    ) -> HashMap<(i64, i64), i64> {
        let mut keys = HashSet::new();
        for (value, weight) in input_state {
            if *weight == 0 {
                continue;
            }
            if let Some(key) = (key_extractor)(value) {
                keys.insert(key);
            }
        }

        let mut expected = HashMap::new();
        for key in keys {
            let mut values = Vec::new();
            for (value, weight) in input_state {
                if *weight == 0 {
                    continue;
                }
                if let Some(value_key) = (key_extractor)(value) {
                    if value_key == key {
                        values.push((*value, *weight));
                    }
                }
            }
            if let Some(aggregate) = (spec.aggregator)(&key, &values) {
                expected.insert((key, aggregate), 1);
            }
        }

        expected
    }

    #[tokio::test]
    async fn aggregate_op_delegates_to_group_by() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "aggregate_input", None)
                .await
                .expect("build input dictionary"),
        );
        let output_dict = Arc::new(
            Dictionary::<(i64, i64)>::with_table(table.clone(), "aggregate_output", None)
                .await
                .expect("build output dictionary"),
        );
        let integrated_dict = Arc::new(
            Dictionary::<(i64, i64)>::with_table(table.clone(), "aggregate_state", None)
                .await
                .expect("build state dictionary"),
        );

        let state = RelationState {
            integrated: VersionedZSet::new(
                integrated_dict.clone(),
                table.clone(),
                "aggregate_state".to_string(),
            )
            .await
            .expect("integrated state"),
            latest_handle: ZSetHandle {
                ns: "aggregate_state".to_string(),
                version: 0,
            },
        };
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "aggregate_output".to_string(),
        )
        .await
        .expect("output");

        let index = IndexedZSet::new(table.clone(), "aggregate_index");
        let key_extractor: KeyExtractor<i64, i64> = Arc::new(|value: &i64| Some(value % 2));
        let spec = AggregateSpec::new("sum", |_key, values| {
            if values.is_empty() {
                return None;
            }
            let mut sum = 0i64;
            for (value, weight) in values {
                sum += value * weight;
            }
            Some(sum)
        });

        let mut op = AggregateOp::new(
            state,
            index,
            table.clone(),
            key_extractor,
            spec,
            output,
        );

        let delta = stage_version(
            input_dict.clone(),
            table.clone(),
            "aggregate_input",
            &[(1, 1), (2, 1), (3, 2)],
        )
        .await;
        let out = op
            .on_step(1, &[delta])
            .await
            .expect("aggregate step")
            .expect("non-empty aggregate");

        let mut cache = std::collections::HashMap::new();
        cache.insert("aggregate_output".to_string(), output_dict.clone());
        let out_materialized =
            crate::stream::util::materialize_zset_handle::<(i64, i64)>(
                table.clone(),
                &mut cache,
                &out,
            )
            .await
            .expect("materialize aggregate output");
        assert_eq!(out_materialized.get(&(1, 7)), Some(&1));
        assert_eq!(out_materialized.get(&(0, 2)), Some(&1));
    }

    #[tokio::test]
    async fn aggregate_op_matches_recompute_for_builtins() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let specs = vec![
            ("count", count_all::<i64, i64>()),
            ("sum", sum_i64_identity::<i64>()),
            ("avg", avg_i64_identity::<i64>()),
            ("min", min_value::<i64, i64>()),
            ("max", max_value::<i64, i64>()),
        ];

        for (suffix, spec) in specs {
            let input_ns = format!("aggregate_input_{}", suffix);
            let output_ns = format!("aggregate_output_{}", suffix);
            let state_ns = format!("aggregate_state_{}", suffix);
            let index_ns = format!("aggregate_index_{}", suffix);

            let input_dict = Arc::new(
                Dictionary::<i64>::with_table(table.clone(), input_ns.clone(), None)
                    .await
                    .expect("build input dictionary"),
            );
            let output_dict = Arc::new(
                Dictionary::<(i64, i64)>::with_table(table.clone(), output_ns.clone(), None)
                    .await
                    .expect("build output dictionary"),
            );
            let integrated_dict = Arc::new(
                Dictionary::<(i64, i64)>::with_table(table.clone(), state_ns.clone(), None)
                    .await
                    .expect("build state dictionary"),
            );

            let state = RelationState {
                integrated: VersionedZSet::new(
                    integrated_dict.clone(),
                    table.clone(),
                    state_ns.clone(),
                )
                .await
                .expect("integrated state"),
                latest_handle: ZSetHandle {
                    ns: state_ns.clone(),
                    version: 0,
                },
            };
            let output =
                VersionedZSet::new(output_dict, table.clone(), output_ns.clone())
                    .await
                    .expect("output");

            let index = IndexedZSet::new(table.clone(), index_ns);
            let key_extractor: KeyExtractor<i64, i64> =
                Arc::new(|value: &i64| Some(value % 2));

            let mut op = AggregateOp::new(
                state,
                index,
                table.clone(),
                key_extractor.clone(),
                spec,
                output,
            );

            let mut input_state: HashMap<i64, i64> = HashMap::new();
            let steps = vec![
                vec![(1, 1), (2, 2), (3, 1)],
                vec![(2, -1), (4, 3)],
                vec![(1, -1), (2, -1), (3, -1), (4, -3)],
            ];

            for (step_idx, deltas) in steps.iter().enumerate() {
                for (value, delta) in deltas {
                    let new_weight = {
                        let entry = input_state.entry(*value).or_insert(0);
                        *entry += *delta;
                        *entry
                    };
                    if new_weight == 0 {
                        input_state.remove(value);
                    }
                }

                let delta_handle = stage_version(
                    input_dict.clone(),
                    table.clone(),
                    &input_ns,
                    deltas,
                )
                .await;
                op.on_step(step_idx as i64, &[delta_handle])
                    .await
                    .expect("aggregate step");

                let expected = recompute_expected(&key_extractor, op.spec(), &input_state);
                let actual = op
                    .inner
                    .state
                    .integrated
                    .materialize()
                    .await
                    .expect("materialize aggregate state");
                assert_eq!(actual, expected, "aggregate mismatch for {}", suffix);
            }
        }
    }

    #[test]
    fn aggregate_specs_compute_expected_values() {
        let values = vec![(1i64, 2), (3i64, 1)];
        let count = count_all::<i64, i64>();
        let sum = sum_i64_identity::<i64>();
        let avg = avg_i64_identity::<i64>();
        let min = min_value::<i64, i64>();
        let max = max_value::<i64, i64>();

        assert_eq!((count.aggregator)(&0, &values), Some(3));
        assert_eq!((sum.aggregator)(&0, &values), Some(5));
        assert_eq!((avg.aggregator)(&0, &values), Some(1));
        assert_eq!((min.aggregator)(&0, &values), Some(1));
        assert_eq!((max.aggregator)(&0, &values), Some(3));
    }
}
