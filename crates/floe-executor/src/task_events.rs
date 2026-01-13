use anyhow::Error;
use tokio::sync::mpsc;

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

pub type GraphTaskSender = mpsc::UnboundedSender<GraphTaskError>;
pub type GraphTaskReceiver = mpsc::UnboundedReceiver<GraphTaskError>;

pub fn report_graph_task_error(
    sender: &GraphTaskSender,
    graph_id: &str,
    task: impl Into<String>,
    error: Error,
) {
    let _ = sender.send(GraphTaskError::new(graph_id, task, error));
}
