use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use floe_core::source::SourceEvent;

use crate::circuit_builder::{RowStreamHandle, SourceRegistry};
use crate::operators::{RowSink, ScanOperator};
use crate::stream_types::{Diff, Row, Timestamp};

/// Represents a decoded row ready to be inserted into a scan operator stream.
#[derive(Debug, Clone)]
pub struct IngestedRow {
    pub handle: RowStreamHandle,
    pub row: Row,
    pub diff: Diff,
    pub timestamp: Timestamp,
}

/// Tracks registered scan operators and routes incoming source events to them.
pub struct ScanRuntime {
    registry: Arc<SourceRegistry>,
    bindings: HashMap<String, RowStreamHandle>,
    default_diff: Diff,
}

impl ScanRuntime {
    pub fn new(registry: Arc<SourceRegistry>) -> Self {
        Self {
            registry,
            bindings: HashMap::new(),
            default_diff: 1,
        }
    }

    pub fn register_scan(
        &mut self,
        source_name: impl Into<String>,
        handle: RowStreamHandle,
    ) -> Result<()> {
        let source_name = source_name.into();
        if !self.registry.contains(&source_name) {
            bail!("source '{source_name}' is not registered in SourceRegistry");
        }
        if self.bindings.insert(source_name.clone(), handle).is_some() {
            bail!("scan for source '{source_name}' already registered");
        }
        Ok(())
    }

    pub fn ingest_event(&self, event: SourceEvent, timestamp: Timestamp) -> Result<IngestedRow> {
        let handle = self
            .bindings
            .get(event.source())
            .copied()
            .ok_or_else(|| anyhow!("no scan registered for source '{}'", event.source()))?;
        let row = self.registry.decode_event(&event)?;
        Ok(IngestedRow {
            handle,
            row,
            diff: self.default_diff,
            timestamp,
        })
    }
}

pub struct ExecutionRuntime<S: RowSink> {
    pub scan_runtime: ScanRuntime,
    scans: HashMap<RowStreamHandle, ScanOperator<S>>,
}

impl<S: RowSink> ExecutionRuntime<S> {
    pub fn new(scan_runtime: ScanRuntime) -> Self {
        Self {
            scan_runtime,
            scans: HashMap::new(),
        }
    }

    pub fn register_bindings<F>(
        &mut self,
        bindings: &[(String, RowStreamHandle)],
        mut builder: F,
    ) -> Result<()>
    where
        F: FnMut(&str, RowStreamHandle) -> ScanOperator<S>,
    {
        for (source, handle) in bindings {
            let operator = builder(source, *handle);
            self.register_scan_operator(source.clone(), *handle, operator)?;
        }
        Ok(())
    }

    pub fn register_scan_operator(
        &mut self,
        source_name: impl Into<String>,
        handle: RowStreamHandle,
        operator: ScanOperator<S>,
    ) -> Result<()> {
        self.scan_runtime
            .register_scan(source_name, handle)
            .context("register scan operator")?;
        self.scans.insert(handle, operator);
        Ok(())
    }

    pub fn process_event(&mut self, event: SourceEvent, timestamp: Timestamp) -> Result<()> {
        let ingested = self.scan_runtime.ingest_event(event, timestamp)?;
        let operator = self
            .scans
            .get_mut(&ingested.handle)
            .ok_or_else(|| anyhow!("no operator registered for handle {:?}", ingested.handle))?;
        operator.ingest(ingested.row, ingested.diff, ingested.timestamp)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;
    use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition};
    use serde_json::json;

    use super::*;
    use crate::circuit_builder::{Circuit, CircuitContext};
    use crate::dataflow_plan::{DataflowPlan, OperatorNode, ScanNode};
    use crate::operators::test_support::TestSink;

    fn bid_definition() -> SourceDefinition {
        SourceDefinition::new(
            "bid",
            vec![
                SourceColumn::new("auction", SourceDataType::Int64),
                SourceColumn::new("bidder", SourceDataType::Int64),
            ],
        )
        .expect("definition")
    }

    #[test]
    fn ingests_decoded_rows_for_registered_scan() {
        let mut registry = SourceRegistry::new();
        registry.register(bid_definition());
        let registry = Arc::new(registry);

        let handle = RowStreamHandle::new(0);

        let mut runtime = ScanRuntime::new(registry.clone());
        runtime.register_scan("bid", handle).expect("register scan");

        let event = SourceEvent::new("bid", json!({"auction": 7, "bidder": 9}));
        let ingested = runtime.ingest_event(event, 42).expect("ingest");
        assert_eq!(ingested.handle, handle);
        assert_eq!(ingested.row[0], ScalarValue::Int64(Some(7)));
        assert_eq!(ingested.row[1], ScalarValue::Int64(Some(9)));
        assert_eq!(ingested.timestamp, 42);
    }

    #[test]
    fn runtime_processes_events_via_scan_operator() {
        let mut registry = SourceRegistry::new();
        registry.register(bid_definition());
        let registry = Arc::new(registry);

        let scan_handle = RowStreamHandle::new(0);
        let scan_runtime = ScanRuntime::new(registry.clone());
        let sink = TestSink::default();
        let operator = ScanOperator::new("bid", sink);

        let mut runtime = ExecutionRuntime::new(scan_runtime);
        runtime
            .register_scan_operator("bid", scan_handle, operator)
            .expect("register operator");

        let event = SourceEvent::new("bid", json!({"auction": 11, "bidder": 22}));
        runtime.process_event(event, 100).expect("process event");

        let operator_sink = runtime.scans.get(&scan_handle).expect("operator").sink();
        assert_eq!(operator_sink.rows.len(), 1);
        assert_eq!(operator_sink.rows[0].0[0], ScalarValue::Int64(Some(11)));
        assert_eq!(operator_sink.rows[0].0[1], ScalarValue::Int64(Some(22)));
        assert_eq!(operator_sink.rows[0].2, 100);
    }

    #[test]
    fn register_bindings_from_context() {
        let mut registry = SourceRegistry::new();
        registry.register(bid_definition());
        let registry = Arc::new(registry);

        let mut circuit = Circuit::new();
        let mut ctx = CircuitContext::new(&mut circuit, registry.clone());
        let mut plan = DataflowPlan::new();
        let scan_id = plan.add_operator(OperatorNode::Scan(ScanNode {
            source_name: "bid".to_string(),
            output: OutputPort::new(OperatorId(0), 0),
        }));
        plan.set_root(scan_id);
        let _ = ctx
            .connect_scan(
                scan_id,
                match &plan.operators[0] {
                    OperatorNode::Scan(node) => node,
                    _ => unreachable!(),
                },
            )
            .expect("connect scan");

        let bindings = ctx.scan_bindings();
        assert_eq!(bindings.len(), 1);

        let scan_runtime = ScanRuntime::new(registry.clone());
        let mut runtime = ExecutionRuntime::new(scan_runtime);
        runtime
            .register_bindings(&bindings, |name, _| {
                ScanOperator::new(name, TestSink::default())
            })
            .expect("register from bindings");
        assert_eq!(runtime.scans.len(), 1);
    }
}
