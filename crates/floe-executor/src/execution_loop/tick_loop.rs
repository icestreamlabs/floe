use std::collections::HashMap;

use anyhow::{Result, anyhow};
use datafusion::scalar::ScalarValue;
use floe_core::source::SourceEvent;

use crate::barrier_clock::{BarrierClock, StepId};
use crate::checkpoint::{
    CheckpointManager, DbspHandleRecord, SourceOffset, handle_kinds, record_if_nonzero,
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
        let (dbsp_handles, source_offsets) = self.seal_checkpoint().await?;
        self.commit_checkpoint(watermark, dbsp_handles, source_offsets)
            .await?;
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

    async fn collect_materialized_view_checkpoints(&mut self) -> Result<Vec<DbspHandleRecord>> {
        let mut entries = Vec::new();
        for operator in self.ops.iter_mut() {
            if let Some(materialize) = operator.as_any_mut().downcast_mut::<MaterializeOperator>() {
                if let Some(mut records) = materialize.checkpoint().await? {
                    entries.append(&mut records);
                }
            }
        }
        Ok(entries)
    }

    async fn collect_operator_checkpoints(&mut self) -> Result<Vec<DbspHandleRecord>> {
        let mut handles = Vec::new();
        for operator in self.ops.iter_mut() {
            if let Some(mut entry) = operator.checkpoint().await? {
                handles.append(&mut entry);
            }
        }
        Ok(handles)
    }

    async fn seal_checkpoint(&mut self) -> Result<(Vec<DbspHandleRecord>, Vec<SourceOffset>)> {
        let mut dbsp_handles = Vec::new();
        dbsp_handles.extend(self.flush_outer_streams().await?);
        let mut operator_handles = self.collect_operator_checkpoints().await?;
        run_barrier_hook(BarrierStage::AfterOperatorFlush)?;
        dbsp_handles.append(&mut operator_handles);
        let mut materialized_views = self.collect_materialized_view_checkpoints().await?;
        run_barrier_hook(BarrierStage::AfterMaterializedViewFlush)?;
        dbsp_handles.append(&mut materialized_views);
        run_barrier_hook(BarrierStage::AfterSealBeforeCommit)?;
        let offsets = self.snapshot_source_offsets();
        Ok((dbsp_handles, offsets))
    }

    fn snapshot_source_offsets(&self) -> Vec<SourceOffset> {
        self.checkpoint
            .as_ref()
            .map(|manager| manager.snapshot_offsets())
            .unwrap_or_default()
    }

    async fn commit_checkpoint(
        &mut self,
        watermark: Timestamp,
        dbsp_handles: Vec<DbspHandleRecord>,
        source_offsets: Vec<SourceOffset>,
    ) -> Result<()> {
        if let Some(manager) = self.checkpoint.as_mut() {
            run_barrier_hook(BarrierStage::AfterOffsetsBeforeCommit)?;
            manager
                .persist(watermark, dbsp_handles, source_offsets)
                .await?;
            run_barrier_hook(BarrierStage::AfterCommit)?;
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

    async fn flush_outer_streams(&mut self) -> Result<Vec<DbspHandleRecord>> {
        if let Some(registry) = self.outer_streams.as_mut() {
            let handles = registry.flush_all().await?;
            Ok(convert_source_handles(handles))
        } else {
            Ok(Vec::new())
        }
    }
}

fn convert_source_handles(handles: Vec<OuterStreamHandle>) -> Vec<DbspHandleRecord> {
    handles
        .into_iter()
        .filter_map(|handle| {
            record_if_nonzero(
                handle_kinds::SOURCE,
                &handle.source,
                &handle.namespace,
                handle.version,
            )
        })
        .collect()
}
