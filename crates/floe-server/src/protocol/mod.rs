mod bootstrap;
mod extended;
mod simple;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use pgwire::api::PgWireServerHandlers;
use pgwire::api::auth::StartupHandler;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};

use super::execution::FloeServerState;
use extended::FloeExtendedHandler;
use simple::FloeQueryHandler;

pub(super) struct FloeServerFactory {
    simple_handler: Arc<FloeQueryHandler>,
    extended_handler: Arc<FloeExtendedHandler>,
}

impl FloeServerFactory {
    pub(super) fn new(state: Arc<FloeServerState>) -> Self {
        let simple_state = Arc::clone(&state);
        Self {
            simple_handler: Arc::new(FloeQueryHandler::new(simple_state)),
            extended_handler: Arc::new(FloeExtendedHandler::new(state)),
        }
    }
}

impl PgWireServerHandlers for FloeServerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.simple_handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.extended_handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.simple_handler.clone()
    }
}
