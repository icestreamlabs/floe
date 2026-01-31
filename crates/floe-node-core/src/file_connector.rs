use std::io::BufRead;
use std::path::PathBuf;

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::connector::{Connector, ConnectorContext, ConnectorTick, run_connector};
use crate::source::SourceEventSender;
use floe_core::source::{SourceDefinition, SourceEvent};

#[derive(Debug, Clone)]
pub struct FileConnectorConfig {
    pub path: PathBuf,
    pub default_source: Option<String>,
}

pub struct FileConnector {
    config: FileConnectorConfig,
    definitions: Vec<SourceDefinition>,
    events: Vec<SourceEvent>,
    cursor: usize,
}

impl FileConnector {
    pub fn new(config: FileConnectorConfig, definitions: Vec<SourceDefinition>) -> Self {
        Self {
            config,
            definitions,
            events: Vec::new(),
            cursor: 0,
        }
    }

    pub async fn run(config: FileConnectorConfig, sender: SourceEventSender) -> Result<()> {
        let mut connector = FileConnector::new(config, Vec::new());
        let ctx = ConnectorContext::new(sender);
        run_connector(&mut connector, &ctx, CancellationToken::new()).await
    }
}

#[async_trait::async_trait]
impl Connector for FileConnector {
    fn name(&self) -> &str {
        "file"
    }

    fn definitions(&self) -> &[SourceDefinition] {
        &self.definitions
    }

    fn tick_interval(&self) -> std::time::Duration {
        std::time::Duration::ZERO
    }

    async fn init(&mut self, _ctx: &ConnectorContext) -> Result<()> {
        self.events = load_events(&self.config).await?;
        self.cursor = 0;
        Ok(())
    }

    async fn tick(&mut self, ctx: &ConnectorContext) -> Result<ConnectorTick> {
        if self.cursor >= self.events.len() {
            return Ok(ConnectorTick::Finished);
        }
        let event = self
            .events
            .get(self.cursor)
            .cloned()
            .context("file connector cursor out of bounds")?;
        self.cursor = self.cursor.saturating_add(1);
        ctx.sender()
            .send(event)
            .await
            .context("failed to send file connector event")?;
        Ok(ConnectorTick::Emitted(1))
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.events.clear();
        Ok(())
    }
}

async fn load_events(config: &FileConnectorConfig) -> Result<Vec<SourceEvent>> {
    let path = config.path.clone();
    let default_source = config.default_source.clone();
    tokio::task::spawn_blocking(move || read_events(path, default_source))
        .await
        .context("join file connector reader")?
}

fn read_events(path: PathBuf, default_source: Option<String>) -> Result<Vec<SourceEvent>> {
    let reader: Box<dyn std::io::BufRead> = if path.as_os_str() == "-" {
        Box::new(std::io::BufReader::new(std::io::stdin()))
    } else {
        let file = std::fs::File::open(&path)
            .with_context(|| format!("open connector input {:?}", path))?;
        Box::new(std::io::BufReader::new(file))
    };

    let mut events = Vec::new();
    for (idx, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("read line {}", idx + 1))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let event = parse_event(trimmed, default_source.as_deref())
            .with_context(|| format!("parse event line {}", idx + 1))?;
        events.push(event);
    }
    Ok(events)
}

fn parse_event(line: &str, default_source: Option<&str>) -> Result<SourceEvent> {
    let value: Value = serde_json::from_str(line).context("decode json line")?;
    let object = value
        .as_object()
        .context("event line must be a JSON object")?;

    if let (Some(source), Some(payload)) = (object.get("source"), object.get("data")) {
        let source = source.as_str().context("event source must be a string")?;
        ensure!(payload.is_object(), "event payload must be an object");
        return Ok(SourceEvent::new(source, payload.clone()));
    }

    let source = default_source.context("event line missing source and no default provided")?;
    ensure!(value.is_object(), "event payload must be an object");
    Ok(SourceEvent::new(source, value))
}
