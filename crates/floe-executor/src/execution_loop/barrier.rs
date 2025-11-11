use anyhow::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BarrierStage {
    AfterOperatorFlush,
    AfterMaterializedViewFlush,
    AfterSealBeforeCommit,
    AfterOffsetsBeforeCommit,
    AfterCommit,
}

pub(crate) fn run_barrier_hook(stage: BarrierStage) -> Result<()> {
    #[cfg(test)]
    {
        barrier_failpoints::maybe_trigger(stage)
    }
    #[cfg(not(test))]
    {
        let _ = stage;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod barrier_failpoints {
    use super::BarrierStage;
    use anyhow::{Result, bail};
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    use std::thread::ThreadId;

    static FAILPOINTS: OnceLock<Mutex<HashMap<ThreadId, BarrierStage>>> = OnceLock::new();

    fn registry() -> &'static Mutex<HashMap<ThreadId, BarrierStage>> {
        FAILPOINTS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub struct FailpointGuard {
        thread: ThreadId,
    }

    impl FailpointGuard {
        pub fn new(stage: BarrierStage) -> Self {
            let thread = std::thread::current().id();
            let mut guard = registry().lock().expect("failpoint registry lock");
            guard.insert(thread, stage);
            Self { thread }
        }
    }

    impl Drop for FailpointGuard {
        fn drop(&mut self) {
            if let Ok(mut guard) = registry().lock() {
                guard.remove(&self.thread);
            }
        }
    }

    pub fn install_failpoint(stage: BarrierStage) -> FailpointGuard {
        FailpointGuard::new(stage)
    }

    pub fn maybe_trigger(stage: BarrierStage) -> Result<()> {
        let thread = std::thread::current().id();
        if let Ok(mut guard) = registry().lock() {
            if let Some(current) = guard.get(&thread).copied() {
                if current == stage {
                    guard.remove(&thread);
                    bail!("barrier failpoint triggered at {:?}", stage);
                }
            }
        }
        Ok(())
    }
}
