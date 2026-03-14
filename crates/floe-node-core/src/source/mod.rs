pub use floe_core::source::{SourceDefinition, SourceEvent, SourceRegistry, SourceResumeToken};
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

#[derive(Clone)]
pub enum SourceEventSender {
    Direct(mpsc::Sender<SourceEventBatch>),
    Routed {
        connector_id: usize,
        sender: mpsc::Sender<RoutedSourceEventBatch>,
    },
}

pub fn channel(capacity: usize) -> (SourceEventSender, SourceEventReceiver) {
    let (sender, receiver) = mpsc::channel(capacity);
    (SourceEventSender::Direct(sender), receiver)
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
) -> SourceEventSender {
    SourceEventSender::Routed {
        connector_id,
        sender,
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
        SourceEventSender::Direct(sender) => sender.send(events).await,
        SourceEventSender::Routed {
            connector_id,
            sender,
        } => sender
            .send(RoutedSourceEventBatch {
                connector_id: *connector_id,
                events,
            })
            .await
            .map_err(|err| SendError(err.0.events)),
    }
}
