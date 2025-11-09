use datafusion::scalar::ScalarValue;

use crate::stream_types::{OperatorId, OutputPort};

#[derive(Debug, Clone)]
pub enum Expr {
    Column(usize),
    Literal(ScalarValue),
    Eq(Box<Expr>, Box<Expr>),
    NotEq(Box<Expr>, Box<Expr>),
    Lt(Box<Expr>, Box<Expr>),
    LtEq(Box<Expr>, Box<Expr>),
    Gt(Box<Expr>, Box<Expr>),
    GtEq(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
    Mod(Box<Expr>, Box<Expr>),
    Neg(Box<Expr>),
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
}

impl Expr {
    pub fn column(index: usize) -> Self {
        Self::Column(index)
    }

    pub fn literal(value: ScalarValue) -> Self {
        Self::Literal(value)
    }
}

#[derive(Debug, Clone)]
pub enum OperatorNode {
    Scan(ScanNode),
    Map(MapNode),
    Filter(FilterNode),
    Join(JoinNode),
    Materialize(MaterializeNode),
}

#[derive(Debug, Clone)]
pub struct ScanNode {
    pub source_name: String,
    pub output: OutputPort,
}

#[derive(Debug, Clone)]
pub struct MapNode {
    pub input: OutputPort,
    pub output: OutputPort,
    pub expressions: Vec<Expr>,
}

#[derive(Debug, Clone)]
pub struct FilterNode {
    pub input: OutputPort,
    pub output: OutputPort,
    pub predicate: Expr,
}

#[derive(Debug, Clone)]
pub struct JoinNode {
    pub left: OutputPort,
    pub right: OutputPort,
    pub output: OutputPort,
    pub on: Vec<(usize, usize)>,
    pub projection: Vec<Expr>,
}

#[derive(Debug, Clone)]
pub struct MaterializeNode {
    pub input: OutputPort,
    pub view_name: String,
}

#[derive(Debug, Clone)]
pub struct DataflowPlan {
    pub operators: Vec<OperatorNode>,
    pub root: OperatorId,
    pub graph_id: String,
}

impl DataflowPlan {
    pub fn new(graph_id: impl Into<String>) -> Self {
        Self {
            operators: Vec::new(),
            root: OperatorId(0),
            graph_id: graph_id.into(),
        }
    }

    pub fn add_operator(&mut self, node: OperatorNode) -> OperatorId {
        let id = OperatorId(self.operators.len());
        self.operators.push(node);
        id
    }

    pub fn set_root(&mut self, root: OperatorId) {
        self.root = root;
    }

    pub fn get(&self, id: OperatorId) -> Option<&OperatorNode> {
        self.operators.get(id.0)
    }

    pub fn get_mut(&mut self, id: OperatorId) -> Option<&mut OperatorNode> {
        self.operators.get_mut(id.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dataflow_plan_tracks_operators_and_root() {
        let mut plan = DataflowPlan::new("test_plan");
        let scan_id = plan.add_operator(OperatorNode::Scan(ScanNode {
            source_name: "bid".to_string(),
            output: OutputPort::new(OperatorId(usize::MAX), 0),
        }));
        assert_eq!(scan_id, OperatorId(0));

        let map_id = plan.add_operator(OperatorNode::Map(MapNode {
            input: OutputPort::new(scan_id, 0),
            output: OutputPort::new(OperatorId(usize::MAX), 0),
            expressions: vec![Expr::column(0)],
        }));
        assert_eq!(map_id, OperatorId(1));

        plan.set_root(map_id);
        assert_eq!(plan.root, map_id);

        if let Some(OperatorNode::Map(map_node)) = plan.get_mut(map_id) {
            map_node
                .expressions
                .push(Expr::literal(ScalarValue::from(1i64)));
        } else {
            panic!("map operator missing");
        }

        match plan.get(map_id) {
            Some(OperatorNode::Map(map_node)) => {
                assert_eq!(map_node.expressions.len(), 2);
                assert!(matches!(map_node.expressions[0], Expr::Column(0)));
            }
            _ => panic!("unexpected operator kind"),
        }
    }
}
