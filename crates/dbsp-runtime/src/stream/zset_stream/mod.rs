mod core;
mod handles;

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use tokio::task::JoinHandle;

use crate::collections::zset::{CompactionPolicy, SegmentRecord, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::core::stream::Stream;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamRetention {
    None,
    KeepLast { keep_last: usize },
    AllButLatest,
}

impl StreamRetention {
    fn window_size(self) -> Option<usize> {
        match self {
            StreamRetention::None => None,
            StreamRetention::KeepLast { keep_last } if keep_last > 0 => Some(keep_last),
            StreamRetention::KeepLast { .. } => None,
            StreamRetention::AllButLatest => Some(1),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionSchedulerConfig {
    pub failure_backoff_ticks: u64,
    pub max_concurrent_jobs: usize,
}

impl Default for CompactionSchedulerConfig {
    fn default() -> Self {
        Self {
            failure_backoff_ticks: 1,
            max_concurrent_jobs: 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CompactionScheduler {
    config: CompactionSchedulerConfig,
    tick: u64,
    next_allowed_tick: u64,
    in_flight_jobs: usize,
}

#[derive(Clone)]
pub(crate) struct CompactionResult {
    pub(crate) source_version: u64,
    pub(crate) segments: Vec<SegmentRecord>,
}

impl CompactionScheduler {
    fn set_config(&mut self, config: CompactionSchedulerConfig) {
        self.config = config;
    }

    fn on_tick(&mut self) {
        self.tick = self.tick.saturating_add(1);
    }

    fn try_start(&mut self) -> bool {
        if self.tick < self.next_allowed_tick {
            return false;
        }
        if self.in_flight_jobs >= self.config.max_concurrent_jobs {
            return false;
        }
        self.in_flight_jobs = self.in_flight_jobs.saturating_add(1);
        true
    }

    fn finish_success(&mut self) {
        self.in_flight_jobs = self.in_flight_jobs.saturating_sub(1);
    }

    fn finish_failure(&mut self) {
        self.in_flight_jobs = self.in_flight_jobs.saturating_sub(1);
        let backoff = self.config.failure_backoff_ticks.max(1);
        self.next_allowed_tick = self.tick.saturating_add(backoff);
    }
}

pub struct ZSetStream<K>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub(crate) stream: Stream<ZSetHandle>,
    delta_stream: Stream<ZSetHandle>,
    versioned: VersionedZSet<K>,
    delta_versioned: VersionedZSet<K>,
    overlay: HashMap<K, i64>,
    retention: StreamRetention,
    compaction: CompactionPolicy,
    compaction_scheduler: CompactionScheduler,
    retention_window: VecDeque<ZSetHandle>,
    retention_counts: HashMap<u64, usize>,
    current_handle: ZSetHandle,
    delta_retention_window: VecDeque<ZSetHandle>,
    delta_retention_counts: HashMap<u64, usize>,
    delta_current_handle: ZSetHandle,
    pending_compaction: Option<JoinHandle<anyhow::Result<CompactionResult>>>,
}

#[cfg(test)]
mod tests {
    use super::{CompactionScheduler, CompactionSchedulerConfig};

    #[test]
    fn scheduler_respects_concurrency_limits() {
        let mut scheduler = CompactionScheduler::default();
        scheduler.set_config(CompactionSchedulerConfig {
            failure_backoff_ticks: 1,
            max_concurrent_jobs: 1,
        });
        scheduler.on_tick();
        assert!(scheduler.try_start());
        assert!(
            !scheduler.try_start(),
            "second job should be blocked by max concurrency"
        );
        scheduler.finish_success();
        assert!(
            scheduler.try_start(),
            "job should start again after completion"
        );
    }

    #[test]
    fn scheduler_enforces_failure_backoff() {
        let mut scheduler = CompactionScheduler::default();
        scheduler.set_config(CompactionSchedulerConfig {
            failure_backoff_ticks: 3,
            max_concurrent_jobs: 1,
        });
        scheduler.on_tick();
        assert!(scheduler.try_start());
        scheduler.finish_failure();

        scheduler.on_tick();
        assert!(!scheduler.try_start(), "backoff tick 1 should block");
        scheduler.on_tick();
        assert!(!scheduler.try_start(), "backoff tick 2 should block");
        scheduler.on_tick();
        assert!(
            scheduler.try_start(),
            "scheduler should allow compaction after backoff"
        );
    }
}
