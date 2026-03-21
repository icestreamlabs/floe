use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use crate::handles::ZSetHandle;
use anyhow::{Context, Result, anyhow};

use super::core::stream::Stream;
use super::cursor::StreamCursor;

#[async_trait::async_trait]
pub trait DeltaOperator: Send {
    /// Called when all inputs have a delta at logical time `ts`.
    /// `inputs[i]` is Delta R_i_t.
    ///
    /// Operators that produce a downstream delta stream should return
    /// `Some(handle)` for every logical tick, using the empty delta handle
    /// (`version = 0`) when the delta at `ts` is empty.
    ///
    /// `None` is reserved for operators that do not emit a downstream handle at
    /// all, such as side-effect-only indexing/sink stages.
    async fn on_step(
        &mut self,
        ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>>;

    /// Stable operator identifier for runtime diagnostics.
    fn operator_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

pub type PipelineExecFut = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
pub type PipelineExec = Box<dyn FnMut(i64, &[ZSetHandle]) -> PipelineExecFut + Send>;
pub type RuntimeErrorHandler = Arc<dyn Fn(anyhow::Error) + Send + Sync + 'static>;

pub fn report_runtime_error(
    handler: &Option<RuntimeErrorHandler>,
    label: &str,
    err: anyhow::Error,
) {
    if let Some(handler) = handler {
        handler(err);
    } else {
        tracing::error!(label, error = %err, "runtime terminated with error");
    }
}

/// Lightweight helper that blocks until all input streams advance to the next
/// frontier, then invokes the supplied callback with the `(timestamp, handle)`
/// pairs. Callers can use this to build dbsp-aware operators that only ever
/// consume/produce `ZSetHandle`s.
pub struct HandleOperatorRuntime<F, Fut>
where
    F: FnMut(i64, &[ZSetHandle]) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    inputs: Vec<StreamCursor<ZSetHandle>>,
    exec: F,
    _phantom: std::marker::PhantomData<Fut>,
}

impl<F, Fut> HandleOperatorRuntime<F, Fut>
where
    F: FnMut(i64, &[ZSetHandle]) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    pub fn new(inputs: Vec<Stream<ZSetHandle>>, exec: F) -> Self {
        let cursors = inputs.into_iter().map(StreamCursor::new).collect();
        Self {
            inputs: cursors,
            exec,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Waits for the next aligned frontier across all inputs, invokes the
    /// callback, and returns the processed timestamp.
    pub async fn step(&mut self) -> Result<i64> {
        let (ts, handles) = self.next_handles().await?;
        (self.exec)(ts, &handles)
            .await
            .with_context(|| format!("execute operator at timestamp {ts}"))?;
        Ok(ts)
    }

    /// Waits for the next aligned frontier across all inputs and returns the
    /// consolidated timestamp and handles.
    pub async fn next_handles(&mut self) -> Result<(i64, Vec<ZSetHandle>)> {
        let mut handles = Vec::with_capacity(self.inputs.len());
        let mut target_ts: Option<i64> = None;

        for cursor in &mut self.inputs {
            let (ts, handle) = cursor.next().await?;
            match target_ts {
                Some(expected) if ts != expected => {
                    return Err(anyhow!(
                        "input frontiers misaligned: expected ts {}, observed {}",
                        expected,
                        ts
                    ));
                }
                Some(_) => {}
                None => target_ts = Some(ts),
            }
            handles.push(handle);
        }

        let ts = target_ts.expect("at least one input stream");
        Ok((ts, handles))
    }
}

pub struct Pipeline {
    runtime: HandleOperatorRuntime<PipelineExec, PipelineExecFut>,
    operators: Vec<Box<dyn DeltaOperator>>,
}

pub struct PipelineBuilder {
    inputs: Vec<Stream<ZSetHandle>>,
    ops: Vec<Box<dyn DeltaOperator>>,
}

impl PipelineBuilder {
    pub fn new(inputs: Vec<Stream<ZSetHandle>>) -> Self {
        Self {
            inputs,
            ops: Vec::new(),
        }
    }

    pub fn push_op<O: DeltaOperator + 'static>(mut self, op: O) -> Self {
        self.ops.push(Box::new(op));
        self
    }

    pub fn ops(mut self, ops: Vec<Box<dyn DeltaOperator>>) -> Self {
        self.ops = ops;
        self
    }

    pub fn build(self) -> Pipeline {
        let runtime = HandleOperatorRuntime::<PipelineExec, PipelineExecFut>::new(
            self.inputs,
            Box::new(|_, _| -> PipelineExecFut { Box::pin(async { Ok(()) }) }),
        );
        Pipeline {
            runtime,
            operators: self.ops,
        }
    }
}

impl Pipeline {
    pub async fn step_once(&mut self) -> anyhow::Result<i64> {
        let (ts, mut current) = self.runtime.next_handles().await?;
        let pipeline_start = Instant::now();
        for (operator_index, op) in self.operators.iter_mut().enumerate() {
            let operator_name = op.operator_name();
            let input_count = current.len();
            let input_versions: Vec<u64> = current.iter().map(|handle| handle.version).collect();
            let op_start = Instant::now();
            let output = op.on_step(ts, &current).await?;
            let operator_elapsed_ms = op_start.elapsed().as_millis() as u64;

            if let Some(out) = output {
                tracing::debug!(
                    ts,
                    operator_index,
                    operator = operator_name,
                    input_count,
                    ?input_versions,
                    output_ns = %out.ns,
                    output_version = out.version,
                    operator_elapsed_ms,
                    "pipeline operator step emitted output"
                );
                current = vec![out];
            } else {
                tracing::debug!(
                    ts,
                    operator_index,
                    operator = operator_name,
                    input_count,
                    ?input_versions,
                    operator_elapsed_ms,
                    "pipeline operator emitted no downstream handle; stopping pipeline for timestamp"
                );
                break;
            }
        }
        tracing::debug!(
            ts,
            pipeline_operator_count = self.operators.len(),
            pipeline_elapsed_ms = pipeline_start.elapsed().as_millis() as u64,
            "pipeline step completed"
        );
        Ok(ts)
    }
}

pub fn single_input_pipeline(
    input: Stream<ZSetHandle>,
    ops: Vec<Box<dyn DeltaOperator>>,
) -> Pipeline {
    PipelineBuilder::new(vec![input]).ops(ops).build()
}
