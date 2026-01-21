use std::sync::Arc;

use anyhow::{Context, Result};
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

use super::{Stream, StreamCore, StreamState};

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

        let initial_default = group.identity().await;
        let state = StreamState::new(initial_default.clone());
        let (frontier_tx, frontier_rx) = watch::channel(state.logical_timestamp);
        let core = Arc::new(StreamCore {
            table,
            namespace: namespace.clone(),
            data_prefix,
            default_prefix,
            state_key,
            group,
            state: std::sync::RwLock::new(state),
            frontier_tx,
        });

        let mut stream = Self { core, frontier_rx };
        let mut needs_initial_flush = false;

        stream.core.clear_intent().await?;

        if let Some(bytes) = stream.table().get(&stream.core.state_key).await? {
            let (timestamp, identity, default, last_default_ts) =
                if let Ok(tuple) = encoding::decode::<(i64, bool, T, i64)>(&bytes) {
                    tuple
                } else {
                    let (timestamp, identity, default) = encoding::decode::<(i64, bool, T)>(&bytes)
                        .context("unable to decode legacy stream state")?;
                    (timestamp, identity, default, timestamp)
                };
            {
                let mut state = stream.write_state();
                state.logical_timestamp = timestamp;
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
