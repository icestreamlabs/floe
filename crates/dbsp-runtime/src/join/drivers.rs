use super::*;
use crate::stream::Stream;
use crate::stream::util::push_value_in_place;

pub(super) async fn drive_join<L, R, O, K>(
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

pub(super) async fn drive_join_transient<L, R, O, K>(
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
pub(super) async fn run_source_driven_join_transient<L, R, O, K>(
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
