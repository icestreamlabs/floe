use anyhow::Error;
use tokio::sync::mpsc::{self, error::TrySendError};

#[derive(Debug)]
pub struct GraphTaskError {
    pub graph_id: String,
    pub task: String,
    pub error: Error,
}

impl GraphTaskError {
    pub fn new(graph_id: impl Into<String>, task: impl Into<String>, error: Error) -> Self {
        Self {
            graph_id: graph_id.into(),
            task: task.into(),
            error,
        }
    }
}

pub const GRAPH_TASK_EVENT_CHANNEL_CAPACITY: usize = 1024;

pub type GraphTaskSender = mpsc::Sender<GraphTaskError>;
pub type GraphTaskReceiver = mpsc::Receiver<GraphTaskError>;

pub fn report_graph_task_error(
    sender: &GraphTaskSender,
    graph_id: &str,
    task: impl Into<String>,
    error: Error,
) {
    let event = GraphTaskError::new(graph_id, task, error);
    match sender.try_send(event) {
        Ok(()) => {}
        Err(TrySendError::Full(event)) => log_unsent_graph_task_error("full", event),
        Err(TrySendError::Closed(event)) => log_unsent_graph_task_error("closed", event),
    }
}

fn log_unsent_graph_task_error(reason: &'static str, event: GraphTaskError) {
    tracing::error!(
        graph_id = %event.graph_id,
        task = %event.task,
        error = %event.error,
        reason,
        "graph background task error could not be queued"
    );
}
