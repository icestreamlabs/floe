use anyhow::Result;
use async_trait::async_trait;
use floe_connector_sdk::{ConnectorContext, ConnectorLifecycle, ConnectorTick, SourceCheckpoint, SourceConnector};
use floe_core::source::SourceDefinition;

pub struct ExampleSourceConnector {
    name: String,
    definitions: Vec<SourceDefinition>,
}

#[async_trait]
impl ConnectorLifecycle for ExampleSourceConnector {
    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait]
impl SourceConnector for ExampleSourceConnector {
    fn source_definitions(&self) -> &[SourceDefinition] {
        &self.definitions
    }

    async fn tick(&mut self, _ctx: &ConnectorContext) -> Result<ConnectorTick> {
        Ok(ConnectorTick::Idle)
    }

    async fn checkpoint(&self) -> Result<Vec<SourceCheckpoint>> {
        Ok(Vec::new())
    }
}
