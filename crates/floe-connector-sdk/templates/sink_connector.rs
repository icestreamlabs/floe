use anyhow::Result;
use async_trait::async_trait;
use floe_connector_sdk::{ConnectorContext, ConnectorLifecycle, SinkCheckpoint, SinkConnector};

pub struct ExampleSinkConnector {
    name: String,
}

#[async_trait]
impl ConnectorLifecycle for ExampleSinkConnector {
    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait]
impl SinkConnector for ExampleSinkConnector {
    async fn send(&mut self, _batch: &[serde_json::Value], _ctx: &ConnectorContext) -> Result<()> {
        Ok(())
    }

    async fn load_checkpoint(&self) -> Result<Option<SinkCheckpoint>> {
        Ok(None)
    }

    async fn persist_checkpoint(&mut self, _cursor: &SinkCheckpoint) -> Result<()> {
        Ok(())
    }
}
