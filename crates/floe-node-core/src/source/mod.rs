pub use floe_core::source::{SourceDefinition, SourceEvent, SourceRegistry, SourceResumeToken};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;

pub type SourceEventBatch = Vec<SourceEvent>;
pub type SourceEventSender = mpsc::Sender<SourceEventBatch>;
pub type SourceEventReceiver = mpsc::Receiver<SourceEventBatch>;

pub fn channel(capacity: usize) -> (SourceEventSender, SourceEventReceiver) {
    mpsc::channel(capacity)
}

pub async fn send_event(
    sender: &SourceEventSender,
    event: SourceEvent,
) -> Result<(), SendError<SourceEventBatch>> {
    sender.send(vec![event]).await
}

pub async fn send_batch(
    sender: &SourceEventSender,
    events: SourceEventBatch,
) -> Result<(), SendError<SourceEventBatch>> {
    if events.is_empty() {
        return Ok(());
    }
    sender.send(events).await
}
