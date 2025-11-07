use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use floe_core::source::{SourceDefinition, SourceEvent};

use crate::dataflow_plan::{
    DataflowPlan, FilterNode, JoinNode, MapNode, MaterializeNode, OperatorNode, ScanNode,
};
use crate::source_decoder::SourceRowDecoder;
use crate::stream_types::{Diff, OperatorId, OutputPort, Row, Timestamp};

/// Lightweight placeholder representing the DBSP circuit under construction.
#[derive(Debug, Default)]
pub struct Circuit {
    next_stream_id: usize,
}

impl Circuit {
    pub fn new() -> Self {
        Self { next_stream_id: 0 }
    }

    fn allocate_stream(&mut self) -> RowStreamHandle {
        let handle = RowStreamHandle::new(self.next_stream_id);
        self.next_stream_id += 1;
        handle
    }
}

/// Typed handle representing the output stream of a connected operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RowStreamHandle {
    id: usize,
    _marker: PhantomData<(Row, Diff)>,
}

impl RowStreamHandle {
    pub(crate) fn new(id: usize) -> Self {
        Self {
            id,
            _marker: PhantomData,
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }
}

/// Registry of data sources that can back Scan operators inside a circuit.
#[derive(Debug, Clone, Default)]
pub struct SourceRegistry {
    sources: HashMap<String, SourceEntry>,
}

#[derive(Debug, Clone)]
struct SourceEntry {
    definition: SourceDefinition,
    decoder: SourceRowDecoder,
}

impl SourceRegistry {
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
        }
    }

    pub fn register(&mut self, definition: SourceDefinition) {
        let decoder = SourceRowDecoder::new(definition.clone());
        let entry = SourceEntry {
            definition,
            decoder,
        };
        self.sources
            .insert(entry.definition.name().to_string(), entry);
    }

    pub fn extend<I>(&mut self, definitions: I)
    where
        I: IntoIterator<Item = SourceDefinition>,
    {
        for definition in definitions {
            self.register(definition);
        }
    }

    pub fn get(&self, name: &str) -> Option<&SourceDefinition> {
        self.sources.get(name).map(|entry| &entry.definition)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.sources.contains_key(name)
    }

    pub fn decoder(&self, name: &str) -> Option<&SourceRowDecoder> {
        self.sources.get(name).map(|entry| &entry.decoder)
    }

    pub fn decode_event(&self, event: &SourceEvent) -> Result<(Row, Option<Timestamp>)> {
        let decoder = self
            .decoder(event.source())
            .with_context(|| format!("source '{}' is not registered", event.source()))?;
        decoder.decode(event)
    }
}

/// Builder responsible for wiring a [`DataflowPlan`] into an executable circuit.
pub struct CircuitContext<'c> {
    circuit: &'c mut Circuit,
    sources: Arc<SourceRegistry>,
    outputs: HashMap<OperatorId, RowStreamHandle>,
    connected: Vec<ConnectedOperator>,
}

impl<'c> CircuitContext<'c> {
    pub fn new(circuit: &'c mut Circuit, sources: Arc<SourceRegistry>) -> Self {
        Self {
            circuit,
            sources,
            outputs: HashMap::new(),
            connected: Vec::new(),
        }
    }

    pub fn build_plan(&mut self, plan: &DataflowPlan) -> Result<RowStreamHandle> {
        for (idx, node) in plan.operators.iter().enumerate() {
            let operator_id = OperatorId(idx);
            let handle = match node {
                OperatorNode::Scan(scan) => self.connect_scan(operator_id, scan)?,
                OperatorNode::Map(map) => self.connect_map(operator_id, map)?,
                OperatorNode::Filter(filter) => self.connect_filter(operator_id, filter)?,
                OperatorNode::Join(join) => self.connect_join(operator_id, join)?,
                OperatorNode::Materialize(materialize) => {
                    self.connect_materialize(operator_id, materialize)?
                }
            };
            if self.outputs.contains_key(&operator_id) {
                bail!("operator {:?} is already connected", operator_id);
            }
            self.outputs.insert(operator_id, handle);
        }

        self.outputs
            .get(&plan.root)
            .copied()
            .ok_or_else(|| anyhow!("root operator {:?} missing output", plan.root))
    }

    pub fn connected(&self) -> &[ConnectedOperator] {
        &self.connected
    }

    pub fn scan_bindings(&self) -> Vec<(String, RowStreamHandle)> {
        self.connected
            .iter()
            .filter_map(|operator| match operator.detail() {
                ConnectedDetail::Scan { source } => Some((source.clone(), operator.output())),
                _ => None,
            })
            .collect()
    }

    pub fn connect_scan(
        &mut self,
        operator_id: OperatorId,
        node: &ScanNode,
    ) -> Result<RowStreamHandle> {
        let source = self
            .sources
            .get(&node.source_name)
            .with_context(|| format!("source '{}' is not registered", node.source_name))?;

        let output = self.circuit.allocate_stream();
        self.connected.push(ConnectedOperator {
            operator_id,
            node: OperatorNode::Scan(node.clone()),
            inputs: Vec::new(),
            output,
            detail: ConnectedDetail::Scan {
                source: source.name().to_string(),
            },
        });
        Ok(output)
    }

    pub fn connect_map(
        &mut self,
        operator_id: OperatorId,
        node: &MapNode,
    ) -> Result<RowStreamHandle> {
        let input = self.resolve_input(&node.input)?;
        let output = self.circuit.allocate_stream();
        self.record_operator(
            operator_id,
            OperatorNode::Map(node.clone()),
            vec![input],
            output,
        );
        Ok(output)
    }

    pub fn connect_filter(
        &mut self,
        operator_id: OperatorId,
        node: &FilterNode,
    ) -> Result<RowStreamHandle> {
        let input = self.resolve_input(&node.input)?;
        let output = self.circuit.allocate_stream();
        self.record_operator(
            operator_id,
            OperatorNode::Filter(node.clone()),
            vec![input],
            output,
        );
        Ok(output)
    }

    pub fn connect_join(
        &mut self,
        operator_id: OperatorId,
        node: &JoinNode,
    ) -> Result<RowStreamHandle> {
        let left = self.resolve_input(&node.left)?;
        let right = self.resolve_input(&node.right)?;
        let output = self.circuit.allocate_stream();
        self.record_operator(
            operator_id,
            OperatorNode::Join(node.clone()),
            vec![left, right],
            output,
        );
        Ok(output)
    }

    pub fn connect_materialize(
        &mut self,
        operator_id: OperatorId,
        node: &MaterializeNode,
    ) -> Result<RowStreamHandle> {
        let input = self.resolve_input(&node.input)?;
        let output = self.circuit.allocate_stream();
        self.connected.push(ConnectedOperator {
            operator_id,
            node: OperatorNode::Materialize(node.clone()),
            inputs: vec![input],
            output,
            detail: ConnectedDetail::Materialize {
                view: node.view_name.clone(),
            },
        });
        Ok(output)
    }

    fn resolve_input(&self, port: &OutputPort) -> Result<RowStreamHandle> {
        self.outputs
            .get(&port.operator)
            .copied()
            .with_context(|| format!("operator {:?} has no connected output", port.operator))
    }

    fn record_operator(
        &mut self,
        operator_id: OperatorId,
        node: OperatorNode,
        inputs: Vec<RowStreamHandle>,
        output: RowStreamHandle,
    ) {
        self.connected.push(ConnectedOperator {
            operator_id,
            node,
            inputs,
            output,
            detail: ConnectedDetail::Passthrough,
        });
    }
}

#[derive(Debug, Clone)]
pub struct ConnectedOperator {
    operator_id: OperatorId,
    node: OperatorNode,
    inputs: Vec<RowStreamHandle>,
    output: RowStreamHandle,
    detail: ConnectedDetail,
}

impl ConnectedOperator {
    pub fn operator_id(&self) -> OperatorId {
        self.operator_id
    }

    pub fn node(&self) -> &OperatorNode {
        &self.node
    }

    pub fn inputs(&self) -> &[RowStreamHandle] {
        &self.inputs
    }

    pub fn output(&self) -> RowStreamHandle {
        self.output
    }

    pub fn detail(&self) -> &ConnectedDetail {
        &self.detail
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectedDetail {
    Scan { source: String },
    Materialize { view: String },
    Passthrough,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, table_scan};
    use datafusion::scalar::ScalarValue;
    use floe_core::source::{SourceColumn, SourceDataType, SourceEvent};
    use serde_json::json;

    use crate::query_planner::QueryPlanner;

    fn bid_schema() -> Schema {
        Schema::new(vec![
            Field::new("auction", DataType::Int64, false),
            Field::new("bidder", DataType::Int64, false),
        ])
    }

    fn bid_definition() -> SourceDefinition {
        SourceDefinition::new(
            "bid",
            vec![
                SourceColumn::new("auction", SourceDataType::Int64),
                SourceColumn::new("bidder", SourceDataType::Int64),
            ],
        )
        .expect("valid definition")
    }

    #[test]
    fn decodes_registered_source_event() {
        let mut registry = SourceRegistry::new();
        registry.register(bid_definition());
        let event = SourceEvent::new(
            "bid",
            json!({
                "auction": 1,
                "bidder": 2,
            }),
        );

        let (row, _) = registry.decode_event(&event).expect("decode");
        assert_eq!(row.len(), 2);
        assert_eq!(row[0], ScalarValue::Int64(Some(1)));
        assert_eq!(row[1], ScalarValue::Int64(Some(2)));
    }

    #[test]
    fn builds_plan_from_query_planner_output() {
        let schema = bid_schema();
        let logical_plan = table_scan(Some("bid"), &schema, None)
            .expect("scan")
            .project(vec![col("auction"), col("bidder")])
            .expect("project")
            .build()
            .expect("plan");

        let planner = QueryPlanner::new();
        let dataflow = planner
            .plan(&logical_plan, "mv_bid")
            .expect("dataflow plan");

        let mut circuit = Circuit::new();
        let mut registry = SourceRegistry::new();
        registry.register(bid_definition());
        let registry = Arc::new(registry);

        let mut ctx = CircuitContext::new(&mut circuit, registry);
        let root = ctx.build_plan(&dataflow).expect("build plan");
        assert_eq!(ctx.connected().len(), dataflow.operators.len());
        let last_output = ctx
            .connected()
            .last()
            .expect("materialize operator")
            .output();
        assert_eq!(root, last_output);
    }

    #[test]
    fn rejects_missing_source() {
        let schema = bid_schema();
        let logical_plan = table_scan(Some("bid"), &schema, None)
            .expect("scan")
            .build()
            .expect("plan");
        let dataflow = QueryPlanner::new()
            .plan(&logical_plan, "mv_bid")
            .expect("plan");

        let mut circuit = Circuit::new();
        let registry = Arc::new(SourceRegistry::new());
        let mut ctx = CircuitContext::new(&mut circuit, registry);
        let err = ctx.build_plan(&dataflow).unwrap_err();
        assert!(err.to_string().contains("source 'bid' is not registered"));
    }

    #[test]
    fn errors_on_missing_upstream_output() {
        let mut plan = DataflowPlan::new();
        let map_id = plan.add_operator(OperatorNode::Map(MapNode {
            input: OutputPort::new(OperatorId(42), 0),
            output: OutputPort::new(OperatorId(usize::MAX), 0),
            expressions: vec![crate::dataflow_plan::Expr::column(0)],
        }));
        plan.set_root(map_id);

        let mut circuit = Circuit::new();
        let registry = Arc::new(SourceRegistry::new());
        let mut ctx = CircuitContext::new(&mut circuit, registry);
        let err = ctx.build_plan(&plan).unwrap_err();
        assert!(err.to_string().contains("operator OperatorId(42)"));
    }
}
