use std::pin::Pin;
use std::future::Future;

use crate::handles::ZSetHandle;
use anyhow::{Context, Result, anyhow};

use super::core::stream::Stream;
use super::cursor::StreamCursor;

#[async_trait::async_trait]
pub trait DeltaOperator: Send {
    /// Called when all inputs have a delta at logical time `ts`.
    /// `inputs[i]` is Delta R_i_t.
    /// Returns `Some(output_handle)` if this op emits Delta O_t (non-empty),
    /// or `None` if it emits nothing.
    async fn on_step(
        &mut self,
        ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>>;
}

pub type PipelineExecFut = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
pub type PipelineExec = Box<dyn FnMut(i64, &[ZSetHandle]) -> PipelineExecFut + Send>;

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
        for op in &mut self.operators {
            if let Some(out) = op.on_step(ts, &current).await? {
                current = vec![out];
            } else {
                break;
            }
        }
        Ok(ts)
    }
}

pub fn single_input_pipeline(
    input: Stream<ZSetHandle>,
    ops: Vec<Box<dyn DeltaOperator>>,
) -> Pipeline {
    PipelineBuilder::new(vec![input]).ops(ops).build()
}
