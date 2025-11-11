use std::future::Future;

use crate::handles::ZSetHandle;
use anyhow::{Context, Result, anyhow};

use super::core::stream::Stream;
use super::cursor::StreamCursor;

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
        (self.exec)(ts, &handles)
            .await
            .with_context(|| format!("execute operator at timestamp {ts}"))?;
        Ok(ts)
    }
}
