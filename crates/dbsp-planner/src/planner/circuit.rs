use std::sync::Arc;

use dbsp_circuit::circuit::plan::DbspNodeKind;
use dbsp_circuit::circuit::schema::RowSchema;

#[derive(Debug, Clone)]
pub struct CircuitNode {
    pub id: usize,
    pub kind: DbspNodeKind,
    pub inputs: Vec<usize>,
    pub output_schema: Arc<RowSchema>,
}

#[derive(Debug, Clone)]
pub struct CircuitPlan {
    pub root: usize,
    pub nodes: Vec<CircuitNode>,
}

impl CircuitPlan {
    pub fn nodes(&self) -> &[CircuitNode] {
        &self.nodes
    }

    pub fn node(&self, id: usize) -> Option<&CircuitNode> {
        self.nodes.iter().find(|node| node.id == id)
    }
}
