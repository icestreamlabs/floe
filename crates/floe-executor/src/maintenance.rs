use std::sync::Arc;

use anyhow::Result;
use dbsp::CompactionSchedulerConfig;
use dbsp::collections::CompactionPolicy;
use dbsp::storage::gc::{GcPolicy, SweepStats};

use crate::dbsp_bridge::{DbspBridge, NamespaceStorageSummary};

pub struct DbspMaintenance {
    bridge: DbspBridge,
}

impl DbspMaintenance {
    pub async fn new(db: Arc<slatedb::Db>) -> Result<Self> {
        Ok(Self {
            bridge: DbspBridge::new(db).await?,
        })
    }

    pub fn set_stream_compaction(
        &mut self,
        policy: CompactionPolicy,
        scheduler: CompactionSchedulerConfig,
    ) {
        self.bridge.set_stream_compaction_policy(policy);
        self.bridge
            .set_stream_compaction_scheduler_config(scheduler);
    }

    pub fn pause(&mut self) {
        self.bridge.pause_maintenance();
    }

    pub async fn inspect_namespace_storage(
        &self,
        namespace: &str,
    ) -> Result<NamespaceStorageSummary> {
        self.bridge.inspect_namespace_storage(namespace).await
    }

    pub async fn compact_namespace_once(&mut self, namespace: &str) -> Result<Option<u64>> {
        self.bridge.compact_namespace_once(namespace).await
    }

    pub async fn run_namespace_gc_once(
        &self,
        namespace: &str,
        policy: GcPolicy,
    ) -> Result<SweepStats> {
        self.bridge.run_namespace_gc_once(namespace, policy).await
    }
}
