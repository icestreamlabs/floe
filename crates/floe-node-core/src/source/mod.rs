pub use floe_core::source::{SourceDefinition, SourceEvent, SourceRegistry, SourceResumeToken};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;

pub type SourceEventBatch = Vec<SourceEvent>;
pub type SourceEventReceiver = mpsc::Receiver<SourceEventBatch>;
pub type RoutedSourceEventReceiver = mpsc::Receiver<RoutedSourceEventBatch>;

#[derive(Debug)]
pub struct RoutedSourceEventBatch {
    pub connector_id: usize,
    pub events: SourceEventBatch,
}

#[derive(Clone, Debug, Default)]
pub struct PendingEventCounter {
    pending: Arc<AtomicUsize>,
}

impl PendingEventCounter {
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
pub enum SourceEventSender {
    Direct {
        sender: mpsc::Sender<SourceEventBatch>,
        pending: PendingEventCounter,
    },
    Routed {
        connector_id: usize,
        sender: mpsc::Sender<RoutedSourceEventBatch>,
        pending: PendingEventCounter,
    },
}

pub fn channel(capacity: usize) -> (SourceEventSender, SourceEventReceiver) {
    let (sender, receiver) = mpsc::channel(capacity);
    (
        SourceEventSender::Direct {
            sender,
            pending: PendingEventCounter::default(),
        },
        receiver,
    )
}

pub fn routed_channel(
    capacity: usize,
) -> (
    mpsc::Sender<RoutedSourceEventBatch>,
    RoutedSourceEventReceiver,
) {
    mpsc::channel(capacity)
}

pub fn routed_sender(
    connector_id: usize,
    sender: mpsc::Sender<RoutedSourceEventBatch>,
    pending: PendingEventCounter,
) -> SourceEventSender {
    SourceEventSender::Routed {
        connector_id,
        sender,
        pending,
    }
}

impl SourceEventSender {
    pub fn pending_events(&self) -> usize {
        match self {
            SourceEventSender::Direct { pending, .. }
            | SourceEventSender::Routed { pending, .. } => pending.pending(),
        }
    }
}

pub async fn send_event(
    sender: &SourceEventSender,
    event: SourceEvent,
) -> Result<(), SendError<SourceEventBatch>> {
    send_batch(sender, vec![event]).await
}

pub async fn send_batch(
    sender: &SourceEventSender,
    events: SourceEventBatch,
) -> Result<(), SendError<SourceEventBatch>> {
    if events.is_empty() {
        return Ok(());
    }
    match sender {
        SourceEventSender::Direct { sender, pending } => {
            let count = events.len();
            pending.record_enqueue(count);
            if let Err(err) = sender.send(events).await {
                pending.record_dequeue(count);
                return Err(err);
            }
            Ok(())
        }
        SourceEventSender::Routed {
            connector_id,
            sender,
            pending,
        } => {
            let count = events.len();
            pending.record_enqueue(count);
            if let Err(err) = sender
                .send(RoutedSourceEventBatch {
                    connector_id: *connector_id,
                    events,
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
