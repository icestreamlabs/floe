use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use anyhow::{Context, Result};
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;

use crate::algebra::AbelianGroup;
use crate::collections::DEFAULT_HOT_KEY_COMPACTION_THRESHOLD;
use crate::collections::zset::VersionedZSet;
use crate::handles::ZSetHandle;
use crate::operators::join::{JoinInputRetention, JoinOp, JoinTransientInputs};
use crate::relation_state::RelationState;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::{
    DeltaOperator, HandleOperatorRuntime, RuntimeErrorHandler, report_runtime_error,
};
use crate::stream::util::{
    build_exact_stream_from_values, collect_values, publish_scheduled_value, push_value_in_place,
};
use crate::stream::{DeltaHandleStream, Stream};

static JOIN_STEP_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
const JOIN_STEP_LOG_SAMPLE_EVERY: u64 = 256;
type JoinObserver<O> = Arc<dyn Fn(i64, Arc<Vec<(O, i64)>>) + Send + Sync + 'static>;

pub const TRANSIENT_JOIN_INPUT_CHANNEL_CAPACITY: usize = 1024;

#[derive(Clone)]
pub struct TransientJoinInputBatch<T, K> {
    pub ts: i64,
    pub deltas: Arc<Vec<(T, i64)>>,
    pub closed_keys: Arc<Vec<(K, i64)>>,
}

struct JoinTransientInputBuffer<T, K> {
    receiver: Option<mpsc::Receiver<TransientJoinInputBatch<T, K>>>,
    pending: BTreeMap<i64, TransientJoinInputBatch<T, K>>,
    replay_cutoff_ts: i64,
}

impl<T, K> JoinTransientInputBuffer<T, K> {
    fn new(
        receiver: Option<mpsc::Receiver<TransientJoinInputBatch<T, K>>>,
        replay_cutoff_ts: i64,
    ) -> Self {
        Self {
            receiver,
            pending: BTreeMap::new(),
            replay_cutoff_ts,
        }
    }

    fn push_batch(&mut self, batch: TransientJoinInputBatch<T, K>) {
        if batch.ts <= self.replay_cutoff_ts {
            return;
        }
        self.pending.insert(batch.ts, batch);
    }

    fn take_pending_for_ts(&mut self, ts: i64) -> Option<TransientJoinInputBatch<T, K>> {
        while self
            .pending
            .first_key_value()
            .is_some_and(|(pending_ts, _)| *pending_ts < ts)
        {
            self.pending.pop_first();
        }
        let current = self.pending.remove(&ts);
        if current.is_some() {
            self.replay_cutoff_ts = self.replay_cutoff_ts.max(ts);
        }
        current
    }

    fn take_for_ts(&mut self, ts: i64) -> Option<TransientJoinInputBatch<T, K>> {
        loop {
            let recv_result = match self.receiver.as_mut() {
                Some(receiver) => receiver.try_recv(),
                None => return self.take_pending_for_ts(ts),
            };
            match recv_result {
                Ok(batch) => self.push_batch(batch),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    self.receiver = None;
                    break;
                }
            }
        }
        self.take_pending_for_ts(ts)
    }

    async fn recv_next_available_ts(&mut self) -> Option<i64> {
        loop {
            if let Some((ts, _)) = self.pending.first_key_value() {
                return Some(*ts);
            }
            let receiver = self.receiver.as_mut()?;
            match receiver.recv().await {
                Some(batch) => self.push_batch(batch),
                None => {
                    self.receiver = None;
                    return None;
                }
            }
        }
    }

    async fn recv_optional_for_ts(
        &mut self,
        ts: i64,
    ) -> Option<Option<TransientJoinInputBatch<T, K>>> {
        loop {
            if let Some(batch) = self.take_pending_for_ts(ts) {
                return Some(Some(batch));
            }
            if self
                .pending
                .first_key_value()
                .is_some_and(|(pending_ts, _)| *pending_ts > ts)
            {
                return Some(None);
            }
            let receiver = self.receiver.as_mut()?;
            match receiver.recv().await {
                Some(batch) => self.push_batch(batch),
                None => {
                    self.receiver = None;
                    return None;
                }
            }
        }
    }
}

struct JoinTransientInputState<L, R, K> {
    left: JoinTransientInputBuffer<L, K>,
    right: JoinTransientInputBuffer<R, K>,
}

impl<L, R, K> JoinTransientInputState<L, R, K> {
    fn new(
        left: Option<mpsc::Receiver<TransientJoinInputBatch<L, K>>>,
        right: Option<mpsc::Receiver<TransientJoinInputBatch<R, K>>>,
        replay_cutoff_ts: i64,
    ) -> Self {
        Self {
            left: JoinTransientInputBuffer::new(left, replay_cutoff_ts),
            right: JoinTransientInputBuffer::new(right, replay_cutoff_ts),
        }
    }

    fn take_for_ts(&mut self, ts: i64) -> JoinTransientInputs<L, R, K> {
        let left = self.left.take_for_ts(ts);
        let right = self.right.take_for_ts(ts);
        JoinTransientInputs {
            left: left.as_ref().map(|batch| Arc::clone(&batch.deltas)),
            right: right.as_ref().map(|batch| Arc::clone(&batch.deltas)),
            left_closed_keys: left.as_ref().map(|batch| Arc::clone(&batch.closed_keys)),
            right_closed_keys: right.as_ref().map(|batch| Arc::clone(&batch.closed_keys)),
        }
    }
}

/// Join wrapper that drives the JoinOp operator over handle streams without requiring aligned timestamps.
pub struct DbspJoin {
    stream: DeltaHandleStream,
}

impl DbspJoin {
    #[allow(clippy::too_many_arguments)]
    pub async fn new_batch_with_state_namespace<L, R, O, K, KL, KR, P, F>(
        left: &DeltaHandleStream,
        right: &DeltaHandleStream,
        state_namespace: Option<String>,
        left_key: KL,
        right_key: KR,
        predicate: P,
        projector: F,
        error_handler: Option<RuntimeErrorHandler>,
    ) -> Result<Self>
    where
        L: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        R: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        O: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        K: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        KL: Fn(&[(L, i64)]) -> Vec<(K, L, i64)> + Send + Sync + Clone + 'static,
        KR: Fn(&[(R, i64)]) -> Vec<(K, R, i64)> + Send + Sync + Clone + 'static,
        P: Fn(&L, &R) -> bool + Send + Sync + Clone + 'static,
        F: Fn(&L, &R) -> O + Send + Sync + Clone + 'static,
    {
        let table = left.table();
        let frontier = left.current_time().max(right.current_time());
        let horizon = left.semantic_horizon().max(right.semantic_horizon());
        let checkpoint_state = state_namespace.is_some();
        let join_id = state_namespace
            .unwrap_or_else(|| NEXT_JOIN_ID.fetch_add(1, Ordering::Relaxed).to_string());

        let left_state_ns = format!("join_left_state_{join_id}");
        let right_state_ns = format!("join_right_state_{join_id}");
        let left_state = if checkpoint_state {
            RelationState::empty(table.clone(), left_state_ns).await?
        } else {
            RelationState::empty_uncheckpointed(table.clone(), left_state_ns).await?
        };
        let right_state = if checkpoint_state {
            RelationState::empty(table.clone(), right_state_ns).await?
        } else {
            RelationState::empty_uncheckpointed(table.clone(), right_state_ns).await?
        };

        let output_ns = format!("join_output_{join_id}");
        let output_dict = Arc::new(
            Dictionary::<O>::with_table(table.clone(), output_ns.clone(), None)
                .await
                .context("create output dictionary for join")?,
        );
        let output = VersionedZSet::new(output_dict, table.clone(), output_ns.clone())
            .await
            .context("create output zset for join")?;
        let left_index = crate::collections::IndexedBatchZSet::with_hot_key_compaction_threshold(
            table.clone(),
            format!("join_left_index_{join_id}"),
            DEFAULT_HOT_KEY_COMPACTION_THRESHOLD,
        );
        let right_index = crate::collections::IndexedBatchZSet::with_hot_key_compaction_threshold(
            table.clone(),
            format!("join_right_index_{join_id}"),
            DEFAULT_HOT_KEY_COMPACTION_THRESHOLD,
        );
        left_index
            .restore_committed_checkpoint()
            .await
            .context("restore committed left join index")?;
        right_index
            .restore_committed_checkpoint()
            .await
            .context("restore committed right join index")?;

        let join_op = Arc::new(AsyncMutex::new(JoinOp::new_batch(
            left_state,
            right_state,
            left_index,
            right_index,
            Arc::new(left_key),
            Arc::new(right_key),
            Arc::new(predicate),
            Arc::new(projector),
            table.clone(),
            output,
            None,
        )));
        let empty_handle = ZSetHandle {
            ns: output_ns.clone(),
            version: 0,
        };

        let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(ZSetHandleGroup {
            default: empty_handle.clone(),
        });

        let left_history = collect_values(left, horizon).await?;
        let right_history = collect_values(right, horizon).await?;
        let mut output_handles = Vec::with_capacity((horizon + 1) as usize);
        for ts in 0..=horizon {
            let handles = vec![
                left_history[ts as usize].clone(),
                right_history[ts as usize].clone(),
            ];
            let out_handle = {
                let mut op_guard = join_op.lock().await;
                op_guard.on_step(ts, &handles).await?
            }
            .unwrap_or_else(|| empty_handle.clone());
            output_handles.push(out_handle);
        }

        let mut stream = build_exact_stream_from_values(
            table.clone(),
            handle_group,
            "join_output_stream/",
            frontier,
            horizon,
            &output_handles,
            empty_handle.clone(),
        )
        .await?;
        stream.flush().await?;
        {
            let mut op_guard = join_op.lock().await;
            op_guard.enable_live_output_replayable();
        }

        let writer = Arc::new(AsyncMutex::new(stream.clone()));

        let op = Arc::clone(&join_op);
        let mut runtime =
            HandleOperatorRuntime::new(vec![left.stream(), right.stream()], move |ts, handles| {
                let op = Arc::clone(&op);
                let writer = Arc::clone(&writer);
                let empty_handle = empty_handle.clone();
                let handles = handles.to_vec();
                Box::pin(async move {
                    if handles.len() != 2 {
                        return Err(anyhow::anyhow!(
                            "join runtime expected 2 handles, got {}",
                            handles.len()
                        ));
                    }
                    if ts <= horizon {
                        let mut writer_guard = writer.lock().await;
                        publish_scheduled_value(&mut writer_guard, ts).await?;
                        return Ok(());
                    }
                    drive_join(&op, &writer, &empty_handle, ts, handles).await
                })
            });

        let error_handler = error_handler.clone();
        tokio::spawn(async move {
            loop {
                if let Err(err) = runtime.step().await {
                    report_runtime_error(&error_handler, "join", err);
                    break;
                }
            }
        });

        Ok(Self {
            stream: DeltaHandleStream::new(stream),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn spawn_transient<L, R, O, K, KL, KR, P, F>(
        left: &DeltaHandleStream,
        right: &DeltaHandleStream,
        left_key: KL,
        right_key: KR,
        predicate: P,
        projector: F,
        observer: JoinObserver<O>,
        error_handler: Option<RuntimeErrorHandler>,
    ) -> Result<()>
    where
        L: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        R: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        O: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        K: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        KL: Fn(&L) -> Option<K> + Send + Sync + Clone + 'static,
        KR: Fn(&R) -> Option<K> + Send + Sync + Clone + 'static,
        P: Fn(&L, &R) -> bool + Send + Sync + Clone + 'static,
        F: Fn(&L, &R) -> O + Send + Sync + Clone + 'static,
    {
        let left_key = move |deltas: &[(L, i64)]| {
            deltas
                .iter()
                .filter_map(|(row, weight)| left_key(row).map(|key| (key, row.clone(), *weight)))
                .collect()
        };
        let right_key = move |deltas: &[(R, i64)]| {
            deltas
                .iter()
                .filter_map(|(row, weight)| right_key(row).map(|key| (key, row.clone(), *weight)))
                .collect()
        };
        Self::spawn_transient_with_inputs_and_retention(
            left,
            right,
            None,
            None,
            false,
            None,
            JoinInputRetention::RetainAll,
            JoinInputRetention::RetainAll,
            left_key,
            right_key,
            predicate,
            projector,
            observer,
            error_handler,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn spawn_transient_with_inputs<L, R, O, K, KL, KR, P, F>(
        left: &DeltaHandleStream,
        right: &DeltaHandleStream,
        left_transient: Option<mpsc::Receiver<TransientJoinInputBatch<L, K>>>,
        right_transient: Option<mpsc::Receiver<TransientJoinInputBatch<R, K>>>,
        prefer_source_driven_runtime: bool,
        state_namespace: Option<String>,
        left_key: KL,
        right_key: KR,
        predicate: P,
        projector: F,
        observer: JoinObserver<O>,
        error_handler: Option<RuntimeErrorHandler>,
    ) -> Result<()>
    where
        L: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        R: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        O: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        K: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        KL: Fn(&L) -> Option<K> + Send + Sync + Clone + 'static,
        KR: Fn(&R) -> Option<K> + Send + Sync + Clone + 'static,
        P: Fn(&L, &R) -> bool + Send + Sync + Clone + 'static,
        F: Fn(&L, &R) -> O + Send + Sync + Clone + 'static,
    {
        let left_key = move |deltas: &[(L, i64)]| {
            deltas
                .iter()
                .filter_map(|(row, weight)| left_key(row).map(|key| (key, row.clone(), *weight)))
                .collect()
        };
        let right_key = move |deltas: &[(R, i64)]| {
            deltas
                .iter()
                .filter_map(|(row, weight)| right_key(row).map(|key| (key, row.clone(), *weight)))
                .collect()
        };
        Self::spawn_transient_with_inputs_and_retention(
            left,
            right,
            left_transient,
            right_transient,
            prefer_source_driven_runtime,
            state_namespace,
            JoinInputRetention::RetainAll,
            JoinInputRetention::RetainAll,
            left_key,
            right_key,
            predicate,
            projector,
            observer,
            error_handler,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn spawn_transient_with_inputs_and_retention<L, R, O, K, KL, KR, P, F>(
        left: &DeltaHandleStream,
        right: &DeltaHandleStream,
        left_transient: Option<mpsc::Receiver<TransientJoinInputBatch<L, K>>>,
        right_transient: Option<mpsc::Receiver<TransientJoinInputBatch<R, K>>>,
        prefer_source_driven_runtime: bool,
        state_namespace: Option<String>,
        left_retention: JoinInputRetention,
        right_retention: JoinInputRetention,
        left_key: KL,
        right_key: KR,
        predicate: P,
        projector: F,
        observer: JoinObserver<O>,
        error_handler: Option<RuntimeErrorHandler>,
    ) -> Result<()>
    where
        L: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        R: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        O: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        K: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        KL: Fn(&[(L, i64)]) -> Vec<(K, L, i64)> + Send + Sync + Clone + 'static,
        KR: Fn(&[(R, i64)]) -> Vec<(K, R, i64)> + Send + Sync + Clone + 'static,
        P: Fn(&L, &R) -> bool + Send + Sync + Clone + 'static,
        F: Fn(&L, &R) -> O + Send + Sync + Clone + 'static,
    {
        let table = left.table();
        let persist_indexes = state_namespace.is_some();
        let join_id = state_namespace
            .unwrap_or_else(|| NEXT_JOIN_ID.fetch_add(1, Ordering::Relaxed).to_string());

        let left_state_ns = format!("join_left_state_{join_id}");
        let right_state_ns = format!("join_right_state_{join_id}");
        let left_state = if persist_indexes {
            RelationState::empty(table.clone(), left_state_ns).await?
        } else {
            RelationState::empty_uncheckpointed(table.clone(), left_state_ns).await?
        };
        let right_state = if persist_indexes {
            RelationState::empty(table.clone(), right_state_ns).await?
        } else {
            RelationState::empty_uncheckpointed(table.clone(), right_state_ns).await?
        };
        let left_index = crate::collections::IndexedBatchZSet::with_hot_key_compaction_threshold(
            table.clone(),
            format!("join_left_index_{join_id}"),
            DEFAULT_HOT_KEY_COMPACTION_THRESHOLD,
        );
        let right_index = crate::collections::IndexedBatchZSet::with_hot_key_compaction_threshold(
            table.clone(),
            format!("join_right_index_{join_id}"),
            DEFAULT_HOT_KEY_COMPACTION_THRESHOLD,
        );
        let left_closed_index =
            crate::collections::IndexedBatchZSet::with_hot_key_compaction_threshold(
                table.clone(),
                format!("join_left_closed_index_{join_id}"),
                DEFAULT_HOT_KEY_COMPACTION_THRESHOLD,
            );
        let right_closed_index =
            crate::collections::IndexedBatchZSet::with_hot_key_compaction_threshold(
                table.clone(),
                format!("join_right_closed_index_{join_id}"),
                DEFAULT_HOT_KEY_COMPACTION_THRESHOLD,
            );
        if persist_indexes {
            left_index
                .restore_committed_checkpoint()
                .await
                .context("restore committed left join index")?;
            right_index
                .restore_committed_checkpoint()
                .await
                .context("restore committed right join index")?;
            left_closed_index
                .restore_committed_checkpoint()
                .await
                .context("restore committed left closed join key index")?;
            right_closed_index
                .restore_committed_checkpoint()
                .await
                .context("restore committed right closed join key index")?;
        }

        let join_op = Arc::new(AsyncMutex::new(
            JoinOp::new_without_output_batch_with_closed_indexes(
                left_state,
                right_state,
                left_index,
                right_index,
                left_closed_index,
                right_closed_index,
                Arc::new(left_key),
                Arc::new(right_key),
                Arc::new(predicate),
                Arc::new(projector),
                table.clone(),
                None,
            )
            .with_input_retention(left_retention, right_retention)
            .with_persist_indexes(persist_indexes),
        ));

        let output_version = Arc::new(AtomicU64::new(0));

        // Rehydrate only through the committed frontier. Any future scheduled
        // handles past that frontier are still replayed by the live runtime as
        // their timestamps are committed upstream.
        let left_history = collect_values(left, left.current_time()).await?;
        let right_history = collect_values(right, right.current_time()).await?;
        let left_default = left.default_value();
        let right_default = right.default_value();
        let replay_len = left_history.len().max(right_history.len());
        let replay_cutoff_ts = left.current_time().max(right.current_time()).max(
            i64::try_from(replay_len)
                .unwrap_or(i64::MAX)
                .saturating_sub(1),
        );
        for ts in 0..replay_len {
            let handles = vec![
                left_history
                    .get(ts)
                    .cloned()
                    .unwrap_or_else(|| left_default.clone()),
                right_history
                    .get(ts)
                    .cloned()
                    .unwrap_or_else(|| right_default.clone()),
            ];
            drive_join_transient(
                &join_op,
                &observer,
                &output_version,
                None,
                None,
                ts as i64,
                handles,
            )
            .await?;
        }

        let use_source_driven_runtime =
            prefer_source_driven_runtime && left_transient.is_some() && right_transient.is_some();
        if use_source_driven_runtime {
            let op = Arc::clone(&join_op);
            let observer_clone = Arc::clone(&observer);
            let version_clone = Arc::clone(&output_version);
            let error_handler = error_handler.clone();
            let left_default_handle = left_default.clone();
            let right_default_handle = right_default.clone();
            let left_transient =
                left_transient.expect("source-driven join requires left transient input");
            let right_transient =
                right_transient.expect("source-driven join requires right transient input");
            tokio::spawn(async move {
                if let Err(err) = run_source_driven_join_transient(
                    op,
                    observer_clone,
                    version_clone,
                    left_default_handle,
                    right_default_handle,
                    left_transient,
                    right_transient,
                    replay_cutoff_ts,
                )
                .await
                {
                    report_runtime_error(&error_handler, "join", err);
                }
            });
            return Ok(());
        }

        let transient_inputs = Arc::new(AsyncMutex::new(JoinTransientInputState::new(
            left_transient,
            right_transient,
            replay_cutoff_ts,
        )));
        let op = Arc::clone(&join_op);
        let observer_clone = Arc::clone(&observer);
        let version_clone = Arc::clone(&output_version);
        let transient_inputs_clone = Arc::clone(&transient_inputs);
        let mut runtime =
            HandleOperatorRuntime::new(vec![left.stream(), right.stream()], move |ts, handles| {
                let op = Arc::clone(&op);
                let observer = Arc::clone(&observer_clone);
                let output_version = Arc::clone(&version_clone);
                let transient_inputs = Arc::clone(&transient_inputs_clone);
                let handles = handles.to_vec();
                Box::pin(async move {
                    if handles.len() != 2 {
                        return Err(anyhow::anyhow!(
                            "join runtime expected 2 handles, got {}",
                            handles.len()
                        ));
                    }
                    let transient_inputs = {
                        let mut state_guard = transient_inputs.lock().await;
                        Some(state_guard.take_for_ts(ts))
                    };
                    drive_join_transient(
                        &op,
                        &observer,
                        &output_version,
                        None,
                        transient_inputs,
                        ts,
                        handles,
                    )
                    .await
                })
            });

        let error_handler = error_handler.clone();
        tokio::spawn(async move {
            loop {
                if let Err(err) = runtime.step().await {
                    report_runtime_error(&error_handler, "join", err);
                    break;
                }
            }
        });

        Ok(())
    }

    pub fn stream(&self) -> DeltaHandleStream {
        self.stream.clone()
    }
}

async fn drive_join<L, R, O, K>(
    op: &Arc<AsyncMutex<JoinOp<L, R, O, K>>>,
    writer: &Arc<AsyncMutex<Stream<ZSetHandle>>>,
    empty_handle: &ZSetHandle,
    ts: i64,
    handles: Vec<ZSetHandle>,
) -> Result<()>
where
    L: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    K: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let trace_enabled = tracing::enabled!(tracing::Level::TRACE);
    let span = trace_enabled.then(|| {
        tracing::trace_span!(
            "join_step",
            ts,
            left_ns = tracing::field::Empty,
            left_version = tracing::field::Empty,
            right_ns = tracing::field::Empty,
            right_version = tracing::field::Empty
        )
    });
    let _enter = span.as_ref().map(|span| span.enter());
    if let Some(span) = span.as_ref() {
        if let Some(left) = handles.first() {
            span.record("left_ns", left.ns.as_str());
            span.record("left_version", left.version);
        }
        if let Some(right) = handles.get(1) {
            span.record("right_ns", right.ns.as_str());
            span.record("right_version", right.version);
        }
    }
    if trace_enabled
        && JOIN_STEP_LOG_COUNTER
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(JOIN_STEP_LOG_SAMPLE_EVERY)
    {
        tracing::trace!("join step");
    }
    let mut op_guard = op.lock().await;
    let out = op_guard
        .on_step(ts, &handles)
        .await?
        .unwrap_or_else(|| empty_handle.clone());
    let mut writer_guard = writer.lock().await;
    push_value_in_place(&mut writer_guard, out);
    writer_guard.flush().await?;
    Ok(())
}

async fn drive_join_transient<L, R, O, K>(
    op: &Arc<AsyncMutex<JoinOp<L, R, O, K>>>,
    observer: &JoinObserver<O>,
    output_version: &Arc<AtomicU64>,
    observer_version_override: Option<i64>,
    transient_inputs: Option<JoinTransientInputs<L, R, K>>,
    ts: i64,
    handles: Vec<ZSetHandle>,
) -> Result<()>
where
    L: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    K: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let span = tracing::enabled!(tracing::Level::TRACE).then(|| {
        tracing::trace_span!(
            "join_step_transient",
            ts,
            left_ns = tracing::field::Empty,
            left_version = tracing::field::Empty,
            right_ns = tracing::field::Empty,
            right_version = tracing::field::Empty
        )
    });
    let _enter = span.as_ref().map(|span| span.enter());
    if let Some(span) = span.as_ref() {
        if let Some(left) = handles.first() {
            span.record("left_ns", left.ns.as_str());
            span.record("left_version", left.version);
        }
        if let Some(right) = handles.get(1) {
            span.record("right_ns", right.ns.as_str());
            span.record("right_version", right.version);
        }
    }
    let mut op_guard = op.lock().await;
    let batch = op_guard
        .on_step_transient_with_inputs(ts, &handles, transient_inputs)
        .await?;
    if let Some(batch) = batch.or_else(|| {
        observer_version_override
            .is_some()
            .then(|| Arc::new(Vec::new()))
    }) {
        let version = observer_version_override.unwrap_or_else(|| {
            let version = output_version
                .fetch_add(1, Ordering::Relaxed)
                .saturating_add(1);
            i64::try_from(version).unwrap_or(i64::MAX)
        });
        observer(version, batch);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_source_driven_join_transient<L, R, O, K>(
    op: Arc<AsyncMutex<JoinOp<L, R, O, K>>>,
    observer: JoinObserver<O>,
    output_version: Arc<AtomicU64>,
    left_default: ZSetHandle,
    right_default: ZSetHandle,
    left_transient: mpsc::Receiver<TransientJoinInputBatch<L, K>>,
    right_transient: mpsc::Receiver<TransientJoinInputBatch<R, K>>,
    replay_cutoff_ts: i64,
) -> Result<()>
where
    L: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    K: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let mut transient_inputs = JoinTransientInputState::new(
        Some(left_transient),
        Some(right_transient),
        replay_cutoff_ts,
    );
    let Some(mut next_ts) = transient_inputs.left.recv_next_available_ts().await else {
        return Ok(());
    };
    let Some(right_start_ts) = transient_inputs.right.recv_next_available_ts().await else {
        return Ok(());
    };
    next_ts = next_ts.max(right_start_ts);
    loop {
        let Some(left_batch) = transient_inputs.left.recv_optional_for_ts(next_ts).await else {
            break;
        };
        let Some(right_batch) = transient_inputs.right.recv_optional_for_ts(next_ts).await else {
            break;
        };
        let empty_left = Arc::new(Vec::new());
        let empty_right = Arc::new(Vec::new());
        let empty_left_closed = Arc::new(Vec::new());
        let empty_right_closed = Arc::new(Vec::new());
        drive_join_transient(
            &op,
            &observer,
            &output_version,
            Some(next_ts.saturating_sub(1)),
            Some(JoinTransientInputs {
                left: Some(
                    left_batch
                        .as_ref()
                        .map(|batch| Arc::clone(&batch.deltas))
                        .unwrap_or_else(|| Arc::clone(&empty_left)),
                ),
                right: Some(
                    right_batch
                        .as_ref()
                        .map(|batch| Arc::clone(&batch.deltas))
                        .unwrap_or_else(|| Arc::clone(&empty_right)),
                ),
                left_closed_keys: Some(
                    left_batch
                        .as_ref()
                        .map(|batch| Arc::clone(&batch.closed_keys))
                        .unwrap_or_else(|| Arc::clone(&empty_left_closed)),
                ),
                right_closed_keys: Some(
                    right_batch
                        .as_ref()
                        .map(|batch| Arc::clone(&batch.closed_keys))
                        .unwrap_or_else(|| Arc::clone(&empty_right_closed)),
                ),
            }),
            next_ts,
            vec![left_default.clone(), right_default.clone()],
        )
        .await?;
        next_ts = next_ts.saturating_add(1);
    }
    Ok(())
}

#[derive(Clone)]
struct ZSetHandleGroup {
    default: ZSetHandle,
}

#[async_trait]
impl AbelianGroup<ZSetHandle> for ZSetHandleGroup {
    async fn add(&self, a: &ZSetHandle, _b: &ZSetHandle) -> ZSetHandle {
        a.clone()
    }

    async fn neg(&self, a: &ZSetHandle) -> ZSetHandle {
        a.clone()
    }

    async fn identity(&self) -> ZSetHandle {
        self.default.clone()
    }
}

static NEXT_JOIN_ID: AtomicUsize = AtomicUsize::new(0);
