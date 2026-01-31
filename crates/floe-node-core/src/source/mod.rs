pub use floe_core::source::{SourceDefinition, SourceEvent, SourceRegistry};
use tokio::sync::mpsc;

pub type SourceEventSender = mpsc::Sender<SourceEvent>;
pub type SourceEventReceiver = mpsc::Receiver<SourceEvent>;

pub fn channel(capacity: usize) -> (SourceEventSender, SourceEventReceiver) {
    mpsc::channel(capacity)
}
