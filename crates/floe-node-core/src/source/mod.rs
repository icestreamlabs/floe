pub use floe_core::source::{
    AppendIngestEvent, AppendIngestResumeToken, SourceDefinition, SourceRegistry,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{Mutex, mpsc, oneshot};

pub type AppendIngestEventBatch = Vec<AppendIngestEvent>;
pub type AppendIngestEventReceiver = mpsc::Receiver<AppendIngestEventBatch>;
pub type RoutedAppendIngestEventReceiver = mpsc::Receiver<RoutedAppendIngestEventBatch>;

#[derive(Debug)]
pub struct RoutedAppendIngestEventBatch {
    pub connector_id: usize,
    pub events: AppendIngestEventBatch,
    pub commit_ack: Option<CommitAck>,
}

pub type CommitAckReceiver = oneshot::Receiver<Result<(), String>>;

#[derive(Debug, Clone)]
pub struct CommitAck {
    inner: Arc<CommitAckInner>,
}

#[derive(Debug)]
struct CommitAckInner {
    remaining: AtomicUsize,
    sender: Mutex<Option<oneshot::Sender<Result<(), String>>>>,
}

impl CommitAck {
    fn new(row_count: usize, sender: oneshot::Sender<Result<(), String>>) -> Self {
        Self {
            inner: Arc::new(CommitAckInner {
                remaining: AtomicUsize::new(row_count),
                sender: Mutex::new(Some(sender)),
            }),
        }
    }

    pub async fn record_committed(&self) {
        if self.inner.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.send(Ok(())).await;
        }
    }

    pub async fn record_failed(&self, message: impl Into<String>) {
        self.send(Err(message.into())).await;
    }

    async fn send(&self, result: Result<(), String>) {
        if let Some(sender) = self.inner.sender.lock().await.take() {
            let _ = sender.send(result);
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct PendingAppendIngestEventCounter {
    pending: Arc<AtomicUsize>,
}

impl PendingAppendIngestEventCounter {
    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }

    pub fn record_enqueue(&self, count: usize) {
        if count == 0 {
            return;
        }
        self.pending.fetch_add(count, Ordering::AcqRel);
    }

    pub fn record_dequeue(&self, count: usize) {
        if count == 0 {
            return;
        }
        self.pending
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(count))
            })
            .ok();
    }
}

#[derive(Clone)]
pub enum AppendIngestEventSender {
    Direct {
        sender: mpsc::Sender<AppendIngestEventBatch>,
        pending: PendingAppendIngestEventCounter,
    },
    Routed {
        connector_id: usize,
        sender: mpsc::Sender<RoutedAppendIngestEventBatch>,
        pending: PendingAppendIngestEventCounter,
    },
}

pub fn channel(capacity: usize) -> (AppendIngestEventSender, AppendIngestEventReceiver) {
    let (sender, receiver) = mpsc::channel(capacity);
    (
        AppendIngestEventSender::Direct {
            sender,
            pending: PendingAppendIngestEventCounter::default(),
        },
        receiver,
    )
}

pub fn routed_channel(
    capacity: usize,
) -> (
    mpsc::Sender<RoutedAppendIngestEventBatch>,
    RoutedAppendIngestEventReceiver,
) {
    mpsc::channel(capacity)
}

pub fn routed_sender(
    connector_id: usize,
    sender: mpsc::Sender<RoutedAppendIngestEventBatch>,
    pending: PendingAppendIngestEventCounter,
) -> AppendIngestEventSender {
    AppendIngestEventSender::Routed {
        connector_id,
        sender,
        pending,
    }
}

impl AppendIngestEventSender {
    pub fn pending_events(&self) -> usize {
        match self {
            AppendIngestEventSender::Direct { pending, .. }
            | AppendIngestEventSender::Routed { pending, .. } => pending.pending(),
        }
    }
}

pub async fn send_event(
    sender: &AppendIngestEventSender,
    event: AppendIngestEvent,
) -> Result<(), SendError<AppendIngestEventBatch>> {
    send_batch(sender, vec![event]).await
}

pub async fn send_batch(
    sender: &AppendIngestEventSender,
    events: AppendIngestEventBatch,
) -> Result<(), SendError<AppendIngestEventBatch>> {
    if events.is_empty() {
        return Ok(());
    }
    match sender {
        AppendIngestEventSender::Direct { sender, pending } => {
            let count = events.len();
            pending.record_enqueue(count);
            if let Err(err) = sender.send(events).await {
                pending.record_dequeue(count);
                return Err(err);
            }
            Ok(())
        }
        AppendIngestEventSender::Routed {
            connector_id,
            sender,
            pending,
        } => {
            let count = events.len();
            pending.record_enqueue(count);
            if let Err(err) = sender
                .send(RoutedAppendIngestEventBatch {
                    connector_id: *connector_id,
                    events,
                    commit_ack: None,
                })
                .await
            {
                pending.record_dequeue(count);
                return Err(SendError(err.0.events));
            }
            Ok(())
        }
    }
}

pub async fn send_batch_with_commit_ack(
    sender: &AppendIngestEventSender,
    events: AppendIngestEventBatch,
) -> Result<CommitAckReceiver, SendError<AppendIngestEventBatch>> {
    if events.is_empty() {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Ok(()));
        return Ok(rx);
    }
    match sender {
        AppendIngestEventSender::Direct { sender, pending } => {
            let count = events.len();
            pending.record_enqueue(count);
            if let Err(err) = sender.send(events).await {
                pending.record_dequeue(count);
                return Err(err);
            }
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(Ok(()));
            Ok(rx)
        }
        AppendIngestEventSender::Routed {
            connector_id,
            sender,
            pending,
        } => {
            let count = events.len();
            let (ack_tx, ack_rx) = oneshot::channel();
            pending.record_enqueue(count);
            if let Err(err) = sender
                .send(RoutedAppendIngestEventBatch {
                    connector_id: *connector_id,
                    events,
                    commit_ack: Some(CommitAck::new(count, ack_tx)),
                })
                .await
            {
                pending.record_dequeue(count);
                return Err(SendError(err.0.events));
            }
            Ok(ack_rx)
        }
    }
}
