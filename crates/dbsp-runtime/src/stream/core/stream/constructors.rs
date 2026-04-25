use std::sync::Arc;

use anyhow::{Context, Result, bail};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::Db;
use tokio::sync::watch;

use crate::algebra::AbelianGroup;
use crate::storage::encoding;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::storage::keyspace::{self, namespace_prefix};
use crate::storage::{KeyValueTable, SlateTable};

use super::{
    Stream, StreamCore, StreamEvaluator, StreamEvaluatorDescriptor, StreamState,
    register_stream_evaluator, registered_stream_evaluator,
};

impl<T> Stream<T>
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
    pub async fn new(
        db: Arc<Db>,
        namespace: impl Into<String>,
        group: Arc<dyn AbelianGroup<T>>,
    ) -> Result<Self> {
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
        Self::with_table(table, namespace, group).await
    }

    pub async fn with_table(
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        group: Arc<dyn AbelianGroup<T>>,
    ) -> Result<Self> {
        let namespace = namespace.into();
        let base = namespace_prefix(keyspace::prefix::STREAM, &namespace);

        let mut data_prefix = base.clone();
        data_prefix.extend_from_slice(b"data/");

        let mut default_prefix = base.clone();
        default_prefix.extend_from_slice(b"default/");

        let mut state_key = base.clone();
        state_key.extend_from_slice(b"meta/state");

        let mut evaluator_key = base.clone();
        evaluator_key.extend_from_slice(b"meta/evaluator");

        let initial_default = group.identity().await;
        let state = StreamState::new(initial_default.clone());
        let (frontier_tx, frontier_rx) = watch::channel(state.logical_timestamp);
        let core = Arc::new(StreamCore {
            table,
            namespace: namespace.clone(),
            data_prefix,
            default_prefix,
            state_key,
            evaluator_key,
            group,
            evaluator: None,
            state: std::sync::RwLock::new(state),
            frontier_tx,
        });

        let mut stream = Self { core, frontier_rx };
        let mut needs_initial_flush = false;

        stream.core.clear_intent().await?;
        if let Some(evaluator_bytes) = stream.table().get_bytes(&stream.core.evaluator_key).await? {
            let evaluator = if let Some(evaluator) = stream
                .rebuild_builtin_evaluator(evaluator_bytes.as_ref())
                .await?
            {
                Some(evaluator)
            } else {
                registered_stream_evaluator::<T>(&namespace)
            };
            let Some(evaluator) = evaluator else {
                bail!(
                    "cannot reopen evaluator-derived stream `{namespace}` without its in-memory DBSP evaluator graph"
                );
            };
            Arc::get_mut(&mut stream.core)
                .expect("new stream should have unique core")
                .evaluator = Some(evaluator);
        }

        if let Some(bytes) = stream.table().get_bytes(&stream.core.state_key).await? {
            let (timestamp, max_known_timestamp, identity, default, last_default_ts) =
                if let Ok(tuple) = encoding::decode::<(i64, i64, bool, T, i64)>(bytes.as_ref()) {
                    tuple
                } else if let Ok(tuple) = encoding::decode::<(i64, bool, T, i64)>(bytes.as_ref()) {
                    let (timestamp, identity, default, last_default_ts) = tuple;
                    (
                        timestamp,
                        timestamp.max(last_default_ts),
                        identity,
                        default,
                        last_default_ts,
                    )
                } else {
                    let (timestamp, identity, default) =
                        encoding::decode::<(i64, bool, T)>(bytes.as_ref())
                            .context("unable to decode legacy stream state")?;
                    (timestamp, timestamp, identity, default, timestamp)
                };
            {
                let mut state = stream.write_state();
                state.logical_timestamp = timestamp;
                state.max_known_timestamp = max_known_timestamp.max(timestamp);
                state.identity = identity;
                state.default = default.clone();
                state.last_default_ts = last_default_ts;
            }
            let default_changes = stream.core.load_default_changes().await?;
            {
                let mut state = stream.write_state();
                state.default_changes = default_changes;
                state.last_default_ts = state.default_changes.keys().copied().max().unwrap_or(0);
                let missing_default = state
                    .default_changes
                    .range(..=state.logical_timestamp)
                    .next_back()
                    .is_none();
                if missing_default {
                    let default_value = state.default.clone();
                    state.default_changes.insert(0, default_value);
                }
            }
            stream.notify_committed_frontier(timestamp);
        } else {
            {
                let mut state = stream.write_state();
                state.default_changes.insert(0, initial_default.clone());
                state.last_default_ts = 0;
                state.pending_defaults.insert(0, initial_default.clone());
                state.pending_state = true;
            }
            needs_initial_flush = true;
        }

        {
            let mut state = stream.write_state();
            state.data_cache.reserve(16);
        }

        if needs_initial_flush {
            stream.flush().await?;
        }

        Ok(stream)
    }

    pub(crate) async fn evaluated_with_table(
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        group: Arc<dyn AbelianGroup<T>>,
        evaluator: Arc<dyn StreamEvaluator<T>>,
    ) -> Result<Self> {
        let namespace = namespace.into();
        register_stream_evaluator(&namespace, evaluator.clone());
        let mut stream = Self::with_table(table, namespace, group).await?;
        Arc::get_mut(&mut stream.core)
            .expect("new evaluated stream should have unique core")
            .evaluator = Some(evaluator);
        stream
            .table()
            .put(&stream.core.evaluator_key, b"ephemeral")
            .await?;
        Ok(stream)
    }

    pub(crate) async fn evaluated_with_table_and_descriptor(
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        group: Arc<dyn AbelianGroup<T>>,
        evaluator: Arc<dyn StreamEvaluator<T>>,
        descriptor: StreamEvaluatorDescriptor,
    ) -> Result<Self> {
        let stream = Self::evaluated_with_table(table, namespace, group, evaluator).await?;
        match descriptor {
            StreamEvaluatorDescriptor::BuiltinTime {
                kind,
                input_namespace,
            } => {
                let encoded = encoding::encode(&(
                    "builtin-time".to_string(),
                    kind.to_string(),
                    input_namespace,
                ))
                .context("encode stream evaluator descriptor")?;
                stream
                    .table()
                    .put(&stream.core.evaluator_key, &encoded)
                    .await?;
            }
            StreamEvaluatorDescriptor::BuiltinUnary {
                kind,
                input_namespace,
            } => {
                let encoded = encoding::encode(&(
                    "builtin-unary".to_string(),
                    kind.to_string(),
                    input_namespace,
                ))
                .context("encode stream evaluator descriptor")?;
                stream
                    .table()
                    .put(&stream.core.evaluator_key, &encoded)
                    .await?;
            }
            StreamEvaluatorDescriptor::BuiltinBinary {
                kind,
                left_namespace,
                right_namespace,
            } => {
                let encoded = encoding::encode(&(
                    "builtin-binary".to_string(),
                    kind.to_string(),
                    left_namespace,
                    right_namespace,
                ))
                .context("encode stream evaluator descriptor")?;
                stream
                    .table()
                    .put(&stream.core.evaluator_key, &encoded)
                    .await?;
            }
        }
        Ok(stream)
    }

    async fn rebuild_builtin_evaluator(
        &self,
        evaluator_bytes: &[u8],
    ) -> Result<Option<Arc<dyn StreamEvaluator<T>>>> {
        if let Ok((family, kind, input_namespace)) =
            encoding::decode::<(String, String, String)>(evaluator_bytes)
        {
            if family == "builtin-time" {
                let input = Box::pin(Stream::with_table(
                    self.table(),
                    input_namespace.clone(),
                    self.group(),
                ))
                .await
                .with_context(|| {
                    format!("rebuild {kind} evaluator input stream `{input_namespace}`")
                })?;
                return Ok(Some(
                    crate::stream::operations::basic::time::builtin_time_evaluator(kind, input)?,
                ));
            }

            if family == "builtin-unary" {
                let input = Box::pin(Stream::with_table(
                    self.table(),
                    input_namespace.clone(),
                    self.group(),
                ))
                .await
                .with_context(|| {
                    format!("rebuild {kind} evaluator input stream `{input_namespace}`")
                })?;
                return Ok(Some(crate::stream::addition::builtin_addition_evaluator(
                    kind,
                    Some(input),
                    None,
                )?));
            }
        }

        if let Ok((family, kind, left_namespace, right_namespace)) =
            encoding::decode::<(String, String, String, String)>(evaluator_bytes)
            && family == "builtin-binary"
        {
            let left = Box::pin(Stream::with_table(
                self.table(),
                left_namespace.clone(),
                self.group(),
            ))
            .await
            .with_context(|| format!("rebuild {kind} evaluator left stream `{left_namespace}`"))?;
            let right = Box::pin(Stream::with_table(
                self.table(),
                right_namespace.clone(),
                self.group(),
            ))
            .await
            .with_context(|| {
                format!("rebuild {kind} evaluator right stream `{right_namespace}`")
            })?;
            return Ok(Some(crate::stream::addition::builtin_addition_evaluator(
                kind,
                Some(left),
                Some(right),
            )?));
        }

        Ok(None)
    }

    pub async fn open_at(
        db: Arc<Db>,
        namespace: impl Into<String>,
        group: Arc<dyn AbelianGroup<T>>,
        frontier: i64,
    ) -> Result<Self> {
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
        Self::open_at_with_table(table, namespace, group, frontier).await
    }

    pub async fn open_at_with_table(
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        group: Arc<dyn AbelianGroup<T>>,
        frontier: i64,
    ) -> Result<Self> {
        let mut stream = Self::with_table(table, namespace, group).await?;
        if frontier > stream.current_time() {
            stream.advance_to(frontier).await?;
        }
        Ok(stream)
    }
}
