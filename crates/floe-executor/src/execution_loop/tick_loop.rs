use std::collections::HashMap;

use anyhow::{Result, anyhow};
use datafusion::scalar::ScalarValue;
use floe_core::source::SourceEvent;

use crate::barrier_clock::{BarrierClock, StepId};
use crate::checkpoint::{
    CheckpointManager, MaterializedViewCheckpointEntry, OperatorCheckpointEntry,
    SourceStreamCheckpointEntry,
};
use crate::circuit_builder::RowStreamHandle;
use crate::operators::{DispatchEvent, EventQueue, MaterializeOperator, ScanOperator};
use crate::outer_stream::{OuterStreamHandle, OuterStreamRegistry};
use crate::stream_types::{Diff, Row, StreamOperator, Timestamp};

use super::barrier::{BarrierStage, run_barrier_hook};
use super::ingest::ExecutionRuntime;

pub struct TickLoop {
    pub(crate) runtime: ExecutionRuntime,
    pub(crate) barrier_clock: BarrierClock,
    pub(crate) ops: Vec<Box<dyn StreamOperator>>,
    pub(crate) queue: EventQueue,
    pub(crate) scan_operators: HashMap<RowStreamHandle, usize>,
    pub(crate) source_watermarks: HashMap<String, Timestamp>,
    pub(crate) outer_streams: Option<OuterStreamRegistry>,
    pub(crate) checkpoint: Option<CheckpointManager>,
}

impl TickLoop {
    pub fn with_graph(
        runtime: ExecutionRuntime,
        ops: Vec<Box<dyn StreamOperator>>,
        queue: EventQueue,
        scan_operators: HashMap<RowStreamHandle, usize>,
        outer_streams: Option<OuterStreamRegistry>,
        checkpoint: Option<CheckpointManager>,
    ) -> Self {
        Self {
            runtime,
            barrier_clock: BarrierClock::new(),
            ops,
            queue,
            scan_operators,
            source_watermarks: HashMap::new(),
            outer_streams,
            checkpoint,
        }
    }

    pub fn register_bindings(&mut self, bindings: &[(String, RowStreamHandle)]) -> Result<()> {
        self.runtime.register_bindings(bindings)?;
        for (source, _) in bindings {
            let initial = self
                .checkpoint
                .as_ref()
                .and_then(|manager| manager.latest_offsets().get(source))
                .copied()
                .unwrap_or(0);
            self.source_watermarks.insert(source.clone(), initial);
            self.record_source_offset(source, initial);
        }
        Ok(())
    }

    pub async fn process_events<I>(&mut self, events: I) -> Result<()>
    where
        I: IntoIterator<Item = (SourceEvent, Timestamp)>,
    {
        for (event, ts) in events {
            let ingested = self.runtime.process_event(event, ts)?;
            self.write_outer_stream(&ingested.source, &ingested.row, ingested.diff)?;
            let operator_index = *self
                .scan_operators
                .get(&ingested.handle)
                .ok_or_else(|| anyhow!("no scan operator for handle {:?}", ingested.handle))?;
            let source = ingested.source.clone();
            let diff = ingested.diff;
            let timestamp = ingested.timestamp;
            self.ingest_into_scan(operator_index, ingested.row, diff, timestamp)?;
            self.record_source_offset(&source, timestamp);
            self.advance_source_watermark(&source, timestamp).await?;
            self.drain_queue()?;
        }
        Ok(())
    }

    fn ingest_into_scan(
        &mut self,
        operator_index: usize,
        row: Row,
        diff: Diff,
        timestamp: Timestamp,
    ) -> Result<()> {
        let scan = self.ops[operator_index]
            .as_any_mut()
            .downcast_mut::<ScanOperator>()
            .ok_or_else(|| anyhow!("operator at index {operator_index} is not a ScanOperator"))?;
        scan.ingest(row, diff, timestamp)
    }

    fn drain_queue(&mut self) -> Result<()> {
        loop {
            let event = {
                let mut queue = self.queue.lock().expect("dispatch queue lock");
                queue.pop_front()
            };
            match event {
                Some(DispatchEvent {
                    target_op_index,
                    input_port,
                    row,
                    diff,
                    ts,
                }) => {
                    self.ops[target_op_index].on_input(input_port, row, diff, ts)?;
                }
                None => break,
            }
        }
        Ok(())
    }

    pub async fn advance_watermark(&mut self, watermark: Timestamp) -> Result<()> {
        if self.barrier_clock.advance(watermark).is_none() {
            return Ok(());
        }
        let step_id = self.barrier_clock.step();
        self.drain_queue()?;
        for operator in self.ops.iter_mut() {
            operator.on_watermark(watermark)?;
        }
        self.seal_step(step_id, watermark).await
    }

    async fn seal_step(&mut self, _step_id: StepId, watermark: Timestamp) -> Result<()> {
        let source_handles = self.flush_outer_streams().await?;
        let operator_states = self.collect_operator_checkpoints().await?;
        run_barrier_hook(BarrierStage::AfterOperatorFlush)?;
        let materialized_views = self.collect_materialized_view_checkpoints().await?;
        run_barrier_hook(BarrierStage::AfterMaterializedViewFlush)?;
        run_barrier_hook(BarrierStage::BeforeManifestWrite)?;
        self.persist_checkpoint(
            watermark,
            operator_states,
            materialized_views,
            source_handles,
        )
        .await?;
        run_barrier_hook(BarrierStage::AfterManifestWrite)?;
        Ok(())
    }

    pub fn current_watermark(&self) -> Timestamp {
        self.barrier_clock.watermark()
    }

    pub async fn advance_source_watermark(
        &mut self,
        source: &str,
        watermark: Timestamp,
    ) -> Result<()> {
        let entry = self
            .source_watermarks
            .get_mut(source)
            .ok_or_else(|| anyhow!("no watermark tracking for source '{source}'"))?;
        if watermark <= *entry {
            return Ok(());
        }
        *entry = watermark;
        let frontier = self.current_frontier();
        self.advance_watermark(frontier).await
    }

    fn current_frontier(&self) -> Timestamp {
        self.source_watermarks
            .values()
            .copied()
            .min()
            .unwrap_or(self.barrier_clock.watermark())
    }

    async fn collect_materialized_view_checkpoints(
        &mut self,
    ) -> Result<Vec<MaterializedViewCheckpointEntry>> {
        let mut entries = Vec::new();
        for operator in self.ops.iter_mut() {
            if let Some(materialize) = operator.as_any_mut().downcast_mut::<MaterializeOperator>() {
                if let Some(entry) = materialize.checkpoint_state().await? {
                    entries.push(entry);
                }
            }
        }
        Ok(entries)
    }

    async fn collect_operator_checkpoints(&mut self) -> Result<Vec<OperatorCheckpointEntry>> {
        let mut entries = Vec::new();
        for (idx, operator) in self.ops.iter_mut().enumerate() {
            if let Some(handles) = operator.checkpoint().await? {
                if !handles.is_empty() {
                    entries.push(OperatorCheckpointEntry {
                        operator_index: idx,
                        handles,
                    });
                }
            }
        }
        Ok(entries)
    }

    async fn persist_checkpoint(
        &mut self,
        watermark: Timestamp,
        operator_states: Vec<OperatorCheckpointEntry>,
        materialized_views: Vec<MaterializedViewCheckpointEntry>,
        source_handles: Vec<OuterStreamHandle>,
    ) -> Result<()> {
        if let Some(manager) = self.checkpoint.as_mut() {
            let coupled = couple_handles_with_offsets(source_handles, manager.latest_offsets());
            manager
                .persist(watermark, operator_states, materialized_views, coupled)
                .await?;
        }
        Ok(())
    }

    fn record_source_offset(&mut self, source: &str, offset: Timestamp) {
        if let Some(manager) = self.checkpoint.as_mut() {
            manager.update_offset(source, offset);
        }
    }

    fn write_outer_stream(&mut self, source: &str, row: &[ScalarValue], diff: Diff) -> Result<()> {
        if let Some(registry) = self.outer_streams.as_mut() {
            if let Some(writer) = registry.writer_mut(source) {
                writer.append(row, diff)?;
            }
        }
        Ok(())
    }

    async fn flush_outer_streams(&mut self) -> Result<Vec<OuterStreamHandle>> {
        if let Some(registry) = self.outer_streams.as_mut() {
            registry.flush_all().await
        } else {
            Ok(Vec::new())
        }
    }
}

fn couple_handles_with_offsets(
    handles: Vec<OuterStreamHandle>,
    offsets: &HashMap<String, u64>,
) -> Vec<SourceStreamCheckpointEntry> {
    handles
        .into_iter()
        .filter_map(|handle| {
            offsets
                .get(&handle.source)
                .copied()
                .map(|offset| SourceStreamCheckpointEntry {
                    source: handle.source,
                    namespace: handle.namespace,
                    version: handle.version,
                    partition: 0,
                    offset,
                })
        })
        .collect()
}
