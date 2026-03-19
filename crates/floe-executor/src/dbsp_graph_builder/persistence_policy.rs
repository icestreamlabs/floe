use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use dbsp::circuit::plan::DbspProjectExpr;
use dbsp::stream::DeltaHandleStream;
use dbsp::{CircuitPlan, DbspNodeKind, DbspPredicate, RowSchema};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PersistenceClass {
    Durable,
    Transient,
}

#[derive(Clone, Debug)]
pub(crate) enum TransientSegmentStep {
    Passthrough,
    Select {
        predicate: DbspPredicate,
        schema: Arc<RowSchema>,
    },
    Project {
        expressions: Arc<Vec<DbspProjectExpr>>,
        schema: Arc<RowSchema>,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct TransientSegmentSpec {
    pub durable_input_idx: usize,
    pub segment_nodes: Vec<usize>,
    pub steps: Vec<TransientSegmentStep>,
    pub score: i32,
}

#[derive(Clone, Debug)]
pub(crate) struct PersistencePolicy {
    classes: HashMap<usize, PersistenceClass>,
    max_transient_segment_nodes: usize,
    min_transient_segment_score: i32,
}

impl PersistencePolicy {
    pub(crate) fn for_plan(plan: &CircuitPlan) -> Self {
        let classes = plan
            .nodes()
            .iter()
            .map(|node| (node.id, classify_node(&node.kind)))
            .collect::<HashMap<_, _>>();
        let max_transient_segment_nodes = std::env::var("FLOE_TRANSIENT_SEGMENT_MAX_NODES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(32);
        let min_transient_segment_score = std::env::var("FLOE_TRANSIENT_SEGMENT_MIN_SCORE")
            .ok()
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(1);
        Self {
            classes,
            max_transient_segment_nodes,
            min_transient_segment_score,
        }
    }

    pub(crate) fn max_transient_segment_nodes(&self) -> usize {
        self.max_transient_segment_nodes
    }

    pub(crate) fn min_transient_segment_score(&self) -> i32 {
        self.min_transient_segment_score
    }

    pub(crate) fn class_for_node(&self, node_idx: usize) -> PersistenceClass {
        self.classes
            .get(&node_idx)
            .copied()
            .unwrap_or(PersistenceClass::Durable)
    }

    pub(crate) fn build_transient_segment(
        &self,
        plan: &CircuitPlan,
        terminal_input_idx: usize,
        built: &HashMap<usize, DeltaHandleStream>,
        allow_terminal_without_consumer: bool,
    ) -> Result<Option<TransientSegmentSpec>> {
        let mut current_idx = terminal_input_idx;
        let mut steps_rev = Vec::new();
        let mut segment_nodes = Vec::new();

        loop {
            if self.class_for_node(current_idx) != PersistenceClass::Transient {
                break;
            }
            let Some(node) = plan.node(current_idx) else {
                return Ok(None);
            };
            let single_consumer = has_single_consumer(plan, current_idx);
            if built.contains_key(&current_idx)
                || (!single_consumer
                    && !(allow_terminal_without_consumer && segment_nodes.is_empty()))
            {
                return Ok(None);
            }

            match &node.kind {
                DbspNodeKind::Passthrough => {
                    segment_nodes.push(current_idx);
                    steps_rev.push(TransientSegmentStep::Passthrough);
                    current_idx = first_input(node, "passthrough")?;
                }
                DbspNodeKind::Select(select) => {
                    segment_nodes.push(current_idx);
                    steps_rev.push(TransientSegmentStep::Select {
                        predicate: select.predicate().clone(),
                        schema: Arc::clone(select.output_schema()),
                    });
                    current_idx = first_input(node, "select")?;
                }
                DbspNodeKind::Project(project) => {
                    segment_nodes.push(current_idx);
                    steps_rev.push(TransientSegmentStep::Project {
                        expressions: Arc::new(project.expressions().to_vec()),
                        schema: Arc::clone(project.input_schema()),
                    });
                    current_idx = first_input(node, "project")?;
                }
                _ => break,
            }
        }

        if steps_rev.is_empty() {
            return Ok(None);
        }

        steps_rev.reverse();
        segment_nodes.reverse();
        if segment_nodes.len() > self.max_transient_segment_nodes {
            return Ok(None);
        }

        let score = steps_rev
            .iter()
            .map(|step| match step {
                TransientSegmentStep::Select { .. } => 3,
                TransientSegmentStep::Project { .. } => 2,
                TransientSegmentStep::Passthrough => 0,
            })
            .sum::<i32>();

        if score < self.min_transient_segment_score {
            return Ok(None);
        }

        Ok(Some(TransientSegmentSpec {
            durable_input_idx: current_idx,
            segment_nodes,
            steps: steps_rev,
            score,
        }))
    }
}

fn classify_node(node: &DbspNodeKind) -> PersistenceClass {
    match node {
        DbspNodeKind::Select(_) | DbspNodeKind::Project(_) | DbspNodeKind::Passthrough => {
            PersistenceClass::Transient
        }
        DbspNodeKind::Source(_)
        | DbspNodeKind::Join(_)
        | DbspNodeKind::Aggregate(_)
        | DbspNodeKind::WindowAggregate(_)
        | DbspNodeKind::TopN(_)
        | DbspNodeKind::Union(_)
        | DbspNodeKind::Distinct(_)
        | DbspNodeKind::Sink(_) => PersistenceClass::Durable,
    }
}

fn first_input(node: &dbsp::CircuitNode, label: &str) -> Result<usize> {
    node.inputs
        .first()
        .copied()
        .with_context(|| anyhow!("{label} node missing required input"))
}

fn has_single_consumer(plan: &CircuitPlan, node_idx: usize) -> bool {
    plan.nodes()
        .iter()
        .flat_map(|node| node.inputs.iter())
        .filter(|&&input| input == node_idx)
        .count()
        == 1
}
