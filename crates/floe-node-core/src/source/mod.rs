use floe_core::source::{SourceDefinition, SourceEvent};
use tokio::sync::mpsc;

pub type SourceEventSender = mpsc::Sender<SourceEvent>;
pub type SourceEventReceiver = mpsc::Receiver<SourceEvent>;

pub fn channel(capacity: usize) -> (SourceEventSender, SourceEventReceiver) {
    mpsc::channel(capacity)
}

#[derive(Default, Debug, Clone)]
pub struct SourceRegistry {
    definitions: Vec<SourceDefinition>,
}

#[allow(dead_code)]
impl SourceRegistry {
    pub fn new() -> Self {
        Self {
            definitions: Vec::new(),
        }
    }

    pub fn register(&mut self, definition: SourceDefinition) {
        self.definitions.push(definition);
    }

    pub fn extend<I>(&mut self, definitions: I)
    where
        I: IntoIterator<Item = SourceDefinition>,
    {
        self.definitions.extend(definitions);
    }

    pub fn definitions(&self) -> &[SourceDefinition] {
        &self.definitions
    }
}
