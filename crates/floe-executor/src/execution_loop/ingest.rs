use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use floe_core::source::SourceEvent;

use crate::circuit_builder::{RowStreamHandle, SourceRegistry};
use crate::stream_types::{Diff, Row, Timestamp};

/// Represents a decoded row ready to be inserted into a scan operator stream.
#[derive(Debug, Clone)]
pub struct IngestedRow {
    pub source: String,
    pub handle: RowStreamHandle,
    pub row: Row,
    pub diff: Diff,
    pub timestamp: Timestamp,
}

/// Tracks registered scan operators and routes incoming source events to them.
pub struct ScanRuntime {
    registry: Arc<SourceRegistry>,
    bindings: HashMap<String, RowStreamHandle>,
    default_diff: Diff,
}

impl ScanRuntime {
    pub fn new(registry: Arc<SourceRegistry>) -> Self {
        Self {
            registry,
            bindings: HashMap::new(),
            default_diff: 1,
        }
    }

    pub fn register_scan(
        &mut self,
        source_name: impl Into<String>,
        handle: RowStreamHandle,
    ) -> Result<()> {
        let source_name = source_name.into();
        if !self.registry.contains(&source_name) {
            bail!("source '{source_name}' is not registered in SourceRegistry");
        }
        if self.bindings.insert(source_name.clone(), handle).is_some() {
            bail!("scan for source '{source_name}' already registered");
        }
        Ok(())
    }

    pub fn ingest_event(
        &self,
        event: SourceEvent,
        fallback_timestamp: Timestamp,
    ) -> Result<IngestedRow> {
        let source_name = event.source().to_string();
        let handle = self
            .bindings
            .get(&source_name)
            .copied()
            .ok_or_else(|| anyhow!("no scan registered for source '{source_name}'"))?;
        let (row, event_timestamp) = self.registry.decode_event(&event)?;
        let timestamp = event_timestamp.unwrap_or(fallback_timestamp);
        Ok(IngestedRow {
            source: source_name,
            handle,
            row,
            diff: self.default_diff,
            timestamp,
        })
    }
}

pub struct ExecutionRuntime {
    pub scan_runtime: ScanRuntime,
}

impl ExecutionRuntime {
    pub fn new(scan_runtime: ScanRuntime) -> Self {
        Self { scan_runtime }
    }

    pub fn register_bindings(&mut self, bindings: &[(String, RowStreamHandle)]) -> Result<()> {
        for (source, handle) in bindings {
            self.scan_runtime
                .register_scan(source.clone(), *handle)
                .context("register scan binding")?;
        }
        Ok(())
    }

    pub fn process_event(
        &mut self,
        event: SourceEvent,
        fallback_timestamp: Timestamp,
    ) -> Result<IngestedRow> {
        self.scan_runtime.ingest_event(event, fallback_timestamp)
    }
}
