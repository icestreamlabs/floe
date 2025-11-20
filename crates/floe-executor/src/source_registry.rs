use std::collections::HashMap;

use anyhow::{Context, Result};
use floe_core::source::{SourceDefinition, SourceEvent};

use crate::source_decoder::SourceRowDecoder;
use crate::stream_types::{Row, Timestamp};

#[derive(Debug, Clone)]
struct SourceEntry {
    definition: SourceDefinition,
    decoder: SourceRowDecoder,
}

/// Registry of known sources and their decoders.
#[derive(Debug, Default, Clone)]
pub struct SourceRegistry {
    sources: HashMap<String, SourceEntry>,
}

impl SourceRegistry {
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
        }
    }

    pub fn register(&mut self, definition: SourceDefinition) {
        let decoder = SourceRowDecoder::new(definition.clone());
        let entry = SourceEntry {
            definition,
            decoder,
        };
        self.sources
            .insert(entry.definition.name().to_string(), entry);
    }

    pub fn extend<I>(&mut self, definitions: I)
    where
        I: IntoIterator<Item = SourceDefinition>,
    {
        for definition in definitions {
            self.register(definition);
        }
    }

    pub fn get(&self, name: &str) -> Option<&SourceDefinition> {
        self.sources.get(name).map(|entry| &entry.definition)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.sources.contains_key(name)
    }

    pub fn decoder(&self, name: &str) -> Option<&SourceRowDecoder> {
        self.sources.get(name).map(|entry| &entry.decoder)
    }

    pub fn decode_event(&self, event: &SourceEvent) -> Result<(Row, Option<Timestamp>)> {
        let decoder = self
            .decoder(event.source())
            .with_context(|| format!("source '{}' is not registered", event.source()))?;
        decoder.decode(event)
    }
}
