use anyhow::{Context, Result};
use futures::StreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, parse_url};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::connector::{Connector, ConnectorContext, ConnectorTick, run_connector};
use crate::event_parser::parse_event_line;
use crate::source::SourceEventSender;
use crate::source::send_event;
use floe_core::source::{SourceDefinition, SourceEvent, SourceResumeToken};

#[derive(Debug, Clone)]
pub struct ObjectStoreConnectorConfig {
    pub url: String,
    pub default_source: Option<String>,
}

pub struct ObjectStoreConnector {
    config: ObjectStoreConnectorConfig,
    definitions: Vec<SourceDefinition>,
    events: Vec<SourceEvent>,
    cursor: usize,
}

impl ObjectStoreConnector {
    pub fn new(config: ObjectStoreConnectorConfig, definitions: Vec<SourceDefinition>) -> Self {
        Self {
            config,
            definitions,
            events: Vec::new(),
            cursor: 0,
        }
    }

    pub async fn run(config: ObjectStoreConnectorConfig, sender: SourceEventSender) -> Result<()> {
        let mut connector = ObjectStoreConnector::new(config, Vec::new());
        let ctx = ConnectorContext::new(sender);
        run_connector(&mut connector, &ctx, CancellationToken::new()).await
    }
}

#[async_trait::async_trait]
impl Connector for ObjectStoreConnector {
    fn name(&self) -> &str {
        "object_store"
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
            .context("object store connector cursor out of bounds")?;
        let cursor = u64::try_from(self.cursor).unwrap_or(u64::MAX);
        self.cursor = self.cursor.saturating_add(1);
        let event = event.with_resume_token(SourceResumeToken::ObjectStore { cursor });
        send_event(ctx.sender(), event)
            .await
            .context("failed to send object store event")?;
        Ok(ConnectorTick::Emitted(1))
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.events.clear();
        Ok(())
    }
}

async fn load_events(config: &ObjectStoreConnectorConfig) -> Result<Vec<SourceEvent>> {
    let url = Url::parse(&config.url).context("parse object store url")?;
    let (store, prefix) = parse_url(&url).context("resolve object store from url")?;
    let default_source = config.default_source.as_deref();

    let mut events = Vec::new();
    let mut listed_any = false;
    let mut stream = store.list(Some(&prefix));
    while let Some(entry) = stream.next().await {
        let entry = entry.context("list object store path")?;
        listed_any = true;
        let mut object_events =
            read_object(store.as_ref(), &entry.location, default_source).await?;
        events.append(&mut object_events);
    }

    if !listed_any {
        let mut object_events = read_object(store.as_ref(), &prefix, default_source).await?;
        events.append(&mut object_events);
    }

    Ok(events)
}

async fn read_object(
    store: &dyn ObjectStore,
    location: &Path,
    default_source: Option<&str>,
) -> Result<Vec<SourceEvent>> {
    let bytes = store
        .get(location)
        .await
        .with_context(|| format!("read object {location}"))?
        .bytes()
        .await
        .context("load object bytes")?;
    let contents =
        String::from_utf8(bytes.to_vec()).context("object contents must be valid utf-8")?;
    let mut events = Vec::new();
    for (idx, line) in contents.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let event = parse_event_line(trimmed, default_source)
            .with_context(|| format!("parse object line {}", idx + 1))?;
        events.push(event);
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::Connector;
    use crate::source::channel;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("floe-object-store-{test_name}-{nanos}"))
    }

    #[tokio::test]
    async fn load_events_reads_single_file_when_prefix_is_object() {
        let root = temp_path("single-file");
        fs::create_dir_all(&root).expect("create temp dir");
        let file = root.join("events.jsonl");
        fs::write(
            &file,
            "{\"source\":\"nexmark_bid\",\"data\":{\"auction\":1}}\n",
        )
        .expect("write events");

        let config = ObjectStoreConnectorConfig {
            url: format!("file://{}", file.display()),
            default_source: None,
        };
        let events = load_events(&config).await.expect("load events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source(), "nexmark_bid");

        fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn connector_tick_emits_default_source_with_resume_tokens() {
        let root = temp_path("tick");
        fs::create_dir_all(&root).expect("create temp dir");
        let file = root.join("events.jsonl");
        fs::write(&file, "{\"auction\":1}\n{\"auction\":2}\n").expect("write events");

        let config = ObjectStoreConnectorConfig {
            url: format!("file://{}", root.display()),
            default_source: Some("nexmark_bid".to_string()),
        };
        let mut connector = ObjectStoreConnector::new(config, Vec::new());
        let (sender, mut rx) = channel(8);
        let ctx = ConnectorContext::new(sender);

        connector.init(&ctx).await.expect("init connector");

        let first = connector.tick(&ctx).await.expect("first tick");
        assert_eq!(first, ConnectorTick::Emitted(1));
        let first_batch = rx.recv().await.expect("first batch");
        assert_eq!(first_batch.len(), 1);
        assert_eq!(first_batch[0].source(), "nexmark_bid");
        assert_eq!(
            first_batch[0].resume_token(),
            Some(&SourceResumeToken::ObjectStore { cursor: 0 })
        );

        let second = connector.tick(&ctx).await.expect("second tick");
        assert_eq!(second, ConnectorTick::Emitted(1));
        let second_batch = rx.recv().await.expect("second batch");
        assert_eq!(
            second_batch[0].resume_token(),
            Some(&SourceResumeToken::ObjectStore { cursor: 1 })
        );

        let done = connector.tick(&ctx).await.expect("finished tick");
        assert_eq!(done, ConnectorTick::Finished);

        connector.shutdown().await.expect("shutdown connector");
        fs::remove_dir_all(root).ok();
    }
}
